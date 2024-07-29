import json
import os
from datetime import timedelta, datetime
from string import Template

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.models.param import Param
from airflow.models.xcom_arg import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

NLP_ADDRESS = os.getenv("NLP_ADDRESS")

LDA_SERVER_ENDPOINT = f"{NLP_ADDRESS}/lda-server"
KEYWORDS_ENDPOINT = f"{LDA_SERVER_ENDPOINT}/keywords"
RELEVANCE_ENDPOINT = f"{LDA_SERVER_ENDPOINT}/relevance"

LANGUAGE_TOOL_ENDPOINT = f"{NLP_ADDRESS}/languagetool"
CHECK_ENDPOINT = f"{LANGUAGE_TOOL_ENDPOINT}/v2/check"

CORRECTOR_ENDPOINT = f"{NLP_ADDRESS}/corrector"
CORRECTION_ENDPOINT = f"{CORRECTOR_ENDPOINT}/correction"

OPENAI_SECRET = os.getenv("OPENAI_SECRET")
OPENAI_GENERATION_ENDPOINT = os.getenv("OPENAI_GENERATION_ENDPOINT")
OPENAI_MODELS_ENDPOINT = os.getenv("OPENAI_MODELS_ENDPOINT")
OPENAI_GENERATION_INPUT_TEMPLATE = Template(
    "<TOPIC>$topic</TOPIC>|<KEYWORDS>$keywords</KEYWORDS>|<CHAPTER_NAME>$chapter</CHAPTER_NAME>:")

LLAMA_GENERATION_ADDRESS = Variable.get("LLAMA_GENERATION_ADDRESS")
LLAMA_GENERATION_ENDPOINT = f"{LLAMA_GENERATION_ADDRESS}/generation"

SUPPORTED_DEFAULT_OPENAI_MODELS = ["gpt-4", "gpt-3.5-turbo", "gpt-3.5-turbo-1106"]
MODEL_OWNER_PREFIX = "user-"
SUPPORTED_LLAMA_MODELS = ["Llama 3 8B Instruct (fine-tuned)"]

OUTPUT_TEMPLATE = Template(
    "### $topic<br>\n**Keywords:** $keywords<br>**Extended keywords:** $extendedKeywords<br>**Corrected mistakes:** `$correctedMistakesNumber`<br>**Relevance score:** `$relevanceScore`<br>**Generated text:**<br>$text")

SUGGESTED_KEYWORDS_VAR_NAME = "SUGGESTED_KEYWORDS"
CHILD_DAG_START_DATE_VAR_NAME = "CHILD_DAG_START_DATE"


def get_openai_headers():
    return {'Authorization': f"Bearer {OPENAI_SECRET}"}


def bound(lower_bound, upper_bound, value):
    return max(lower_bound, min(upper_bound, value))


def get_openai_generation_body(parameters):
    keywords_str = ", ".join(parameters['extended_keywords'])
    user_content = OPENAI_GENERATION_INPUT_TEMPLATE.substitute(topic=parameters['topic'], keywords=keywords_str,
                                                               chapter=parameters['chapter'])

    return {
        "model": parameters['model']['modelId'],
        "messages": [
            {
                "role": "system",
                "content": "Асистент є системою генерації текстів українською мовою за заданим контекстом"
            },
            {
                "role": "user",
                "content": user_content
            }
        ],
        "max_tokens": parameters['max_tokens'],
        "presence_penalty": bound(-2.0, 2.0, parameters['presence_penalty'])
    }


def get_llama_generation_body(parameters):
    return {
        "topic": parameters['topic'],
        "keywords": ", ".join(parameters['extended_keywords']),
        "chapter": parameters['chapter'],
        "max_length": parameters['max_tokens'],
        "repetition_penalty": bound(1.0, 2.0, parameters['presence_penalty'])
    }


def get_check_form(parameters):
    return {
        "language": "uk-UA",
        "text": parameters['generated_text']
    }


def get_correction_body(parameters):
    return {
        "matches": parameters["check_matches"],
        "text": parameters['generated_text']
    }


def get_relevance_body(parameters, extended=False):
    return {
        "keywords": parameters["extended_keywords"] if extended else parameters["keywords"],
        "text": parameters['generated_text']
    }


def get_models():
    openai_models = [{"provider": "OPEN_AI", "modelId": modelId} for modelId in get_openai_models()]
    llama_models = [{"provider": "CUSTOM_LLAMA", "modelId": modelId} for modelId in SUPPORTED_LLAMA_MODELS]
    return llama_models + openai_models


def get_model_values_display(models):
    display_values = {}

    for model in models:
        display_values[json.dumps(model)] = model['modelId']

    return display_values


def get_openai_models():
    headers = get_openai_headers()
    response = requests.get(OPENAI_MODELS_ENDPOINT, headers=headers)
    models = response.json()['data']
    return [model['id'] for model in list(filter(
        lambda model: (model['id'] in SUPPORTED_DEFAULT_OPENAI_MODELS) or model['owned_by'].startswith(
            MODEL_OWNER_PREFIX),
        models))]


def get_parameters(**kwargs):
    models = get_models()

    parameters = {
        "topic": Param(
            type="string",
            title="Topic"
        ),
        "keywords": Param(
            type="string",
            title="Keywords",
            description="Coma-separated list of keywords",
        ),
        "chapter": Param(
            type="string",
            title="Chapter"
        ),
        "model": Param(
            type="string",
            title="Select model to use",
            enum=[json.dumps(model) for model in models],
            values_display=get_model_values_display(models)
        ),
        "max_tokens": Param(
            2000,
            type=["number"],
            title="Maximum number of new tokens to generate",
        ),
        "presence_penalty": Param(
            0.0,
            type=["number"],
            title="Presence / repetition penalty",
        ),
        "keywords_extension_enabled": Param(
            True,
            type="boolean",
            title="Execute keywords extension",
        ),
        "manual_keywords_selection_enabled": Param(
            True,
            type="boolean",
            title="Manual keywords selection",
        ),
        "keywords_extension_topic_threshold": Param(
            0.01,
            type=["number"],
            title="Keywords extension topic threshold",
        ),
        "keywords_extension_word_threshold": Param(
            0.01,
            type=["number"],
            title="Keywords extension word threshold",
        )
    }
    return parameters


def deserialize_model_parameter(**kwargs):
    parameters = kwargs["params"]
    parameters['model'] = json.loads(parameters['model'])
    return parameters


def call_lda_server_to_extend_keywords(parameters, **kwargs):
    parameters["keywords"] = [keyword.strip() for keyword in parameters["keywords"].split(",")]
    if parameters["keywords_extension_enabled"]:
        response = requests.post(KEYWORDS_ENDPOINT, json={"keywords": parameters["keywords"],
                                                          "topic_threshold": parameters[
                                                              "keywords_extension_topic_threshold"],
                                                          "word_threshold": parameters[
                                                              "keywords_extension_word_threshold"]})
        extended_keywords = response.json().get("keywords", [])
        parameters["extended_keywords"] = extended_keywords
    else:
        parameters["extended_keywords"] = parameters["keywords"]
    return parameters


def save_extended_keywords_to_var(parameters, **kwargs):
    Variable.set(SUGGESTED_KEYWORDS_VAR_NAME, ",".join(parameters["extended_keywords"]))
    execution_date = kwargs['execution_date']
    Variable.set(CHILD_DAG_START_DATE_VAR_NAME, execution_date.isoformat())
    return parameters


def choose_keywords_selection_branch(parameters, **kwargs):
    if parameters["manual_keywords_selection_enabled"]:
        return "wait_for_keywords_selection"
    else:
        return "skip_selection"


def get_execution_date(execution_date):
    return datetime.fromisoformat(Variable.get(CHILD_DAG_START_DATE_VAR_NAME))


def load_selected_keywords_from_var(parameters, **kwargs):
    selected_keywords = Variable.get(SUGGESTED_KEYWORDS_VAR_NAME)
    parameters["extended_keywords"] = selected_keywords.split(",")
    return parameters


def generate_openai_text(parameters):
    headers = get_openai_headers()
    body = get_openai_generation_body(parameters)
    response = requests.post(OPENAI_GENERATION_ENDPOINT, headers=headers, json=body)
    generated_text = response.json()['choices'][0]['message']['content']
    return generated_text


def generate_llama_text(parameters):
    body = get_llama_generation_body(parameters)
    response = requests.post(LLAMA_GENERATION_ENDPOINT, json=body)
    generated_text = response.json()['text']
    return generated_text


def generate_text(parameters, **kwargs):
    model = parameters['model']
    provider = model['provider']

    if provider == "OPEN_AI":
        text = generate_openai_text(parameters)
    elif provider == "CUSTOM_LLAMA":
        text = generate_llama_text(parameters)
    else:
        raise Exception('No config for model found')

    parameters["generated_text"] = text
    return parameters


def call_languagetool_server(parameters, **kwargs):
    check_response = requests.post(CHECK_ENDPOINT, data=get_check_form(parameters))
    matches = check_response.json()['matches']
    parameters['check_matches'] = matches

    correction_response = requests.post(CORRECTION_ENDPOINT, json=get_correction_body(parameters))
    parameters['corrected_text'] = correction_response.json()['text']

    return parameters


def call_lda_server_for_relevance_score(parameters, **kwargs):
    response = requests.post(RELEVANCE_ENDPOINT, json=get_relevance_body(parameters))
    relevance_score = response.json().get("score", 0)
    parameters["relevance_score"] = relevance_score

    response_extended = requests.post(RELEVANCE_ENDPOINT, json=get_relevance_body(parameters, extended=True))
    relevance_score_extended = response_extended.json().get("score", 0)
    parameters["relevance_score_extended"] = relevance_score_extended
    return parameters


def save_results(parameters, **kwargs):
    matches = parameters['check_matches']
    parameters['number_of_corrected_errors'] = len(matches)

    result = OUTPUT_TEMPLATE.substitute(
        topic=parameters['topic'],
        keywords=", ".join(parameters['keywords']),
        extendedKeywords=", ".join(parameters['extended_keywords']),
        correctedMistakesNumber=parameters['number_of_corrected_errors'],
        relevanceScore=parameters['relevance_score'],
        text=parameters['corrected_text']
    )

    print("Final parameters with results:", parameters)
    return result


# Define the default arguments and the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

dag = DAG(
    'generation_flow',
    default_args=default_args,
    description='Main generation flow DAG',
    schedule_interval=None,
    params=get_parameters()
)

deserialize_model_parameter_task = PythonOperator(
    task_id='deserialize_model_parameter',
    python_callable=deserialize_model_parameter,
    provide_context=True,
    dag=dag,
)

extend_keywords_task = PythonOperator(
    task_id='extend_keywords',
    python_callable=call_lda_server_to_extend_keywords,
    provide_context=True,
    op_kwargs={'parameters': XComArg(deserialize_model_parameter_task)},
    dag=dag,
)

save_extended_keywords_task = PythonOperator(
    task_id='save_extended_keywords',
    python_callable=save_extended_keywords_to_var,
    provide_context=True,
    op_kwargs={'parameters': XComArg(extend_keywords_task)},
    dag=dag,
)

selection_branch_task = BranchPythonOperator(
    task_id='branch_task',
    python_callable=choose_keywords_selection_branch,
    provide_context=True,
    op_kwargs={'parameters': XComArg(save_extended_keywords_task)},
    dag=dag,
)

wait_for_selection = ExternalTaskSensor(
    task_id='wait_for_keywords_selection',
    external_dag_id='select_keywords',
    external_task_id='save_selected_keywords',
    timeout=600,
    poll_interval=5,
    execution_date_fn=get_execution_date,
    dag=dag
)

skip_selection_task = EmptyOperator(
    task_id='skip_selection',
    dag=dag,
)

merge_task = EmptyOperator(
    task_id='merge_task',
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

load_selected_keywords_task = PythonOperator(
    task_id='load_selected_keywords',
    python_callable=load_selected_keywords_from_var,
    provide_context=True,
    op_kwargs={'parameters': XComArg(extend_keywords_task)},
    dag=dag,
)

llm_text_generation_task = PythonOperator(
    task_id='llm_text_generation',
    python_callable=generate_text,
    provide_context=True,
    op_kwargs={'parameters': XComArg(load_selected_keywords_task)},
    dag=dag,
)

fix_mistakes_task = PythonOperator(
    task_id='fix_mistakes',
    python_callable=call_languagetool_server,
    provide_context=True,
    op_kwargs={'parameters': XComArg(llm_text_generation_task)},
    dag=dag,
)

calculate_relevance_task = PythonOperator(
    task_id='calculate_relevance',
    python_callable=call_lda_server_for_relevance_score,
    provide_context=True,
    op_kwargs={'parameters': XComArg(fix_mistakes_task)},
    dag=dag,
)

save_results_task = PythonOperator(
    task_id='save_results',
    python_callable=save_results,
    provide_context=True,
    op_kwargs={'parameters': XComArg(calculate_relevance_task)},
    dag=dag,
)

deserialize_model_parameter_task >> extend_keywords_task >> save_extended_keywords_task >> selection_branch_task
selection_branch_task >> wait_for_selection >> merge_task
selection_branch_task >> skip_selection_task >> merge_task
merge_task >> load_selected_keywords_task >> llm_text_generation_task >> fix_mistakes_task >> calculate_relevance_task >> save_results_task
