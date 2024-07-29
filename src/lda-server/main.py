import json
from http.server import BaseHTTPRequestHandler, HTTPServer

import spacy
from gensim import corpora
from gensim.models.ldamodel import LdaModel
from gensim.parsing.preprocessing import preprocess_string

model_path = "data/lda.model"
dictionary_path = "data/lda.model.id2word"
topic_threshold = 0.01
word_threshold = 0.01

lda = LdaModel.load(model_path)
print('initialized LDA')
dictionary = corpora.Dictionary.load(dictionary_path)
print('initialized Dictionary')
nlp = spacy.load("uk_core_news_lg", enable=["tok2vec", "lemmatizer"])
print('initialized NLP')


def topic_relevance(lda_model, keywords, num_topics, topic_threshold=topic_threshold, topn=50):
    keyword_set = set(keywords)
    topic_scores = []

    for topic_id, words in lda_model.show_topics(formatted=False, num_topics=num_topics, num_words=topn):
        topic_words = dict(words)
        common_words = keyword_set.intersection(set(topic_words.keys()))
        score = sum(topic_words[word] for word in common_words)
        if score > topic_threshold:
            topic_scores.append((topic_id, score, common_words))

    return sorted(topic_scores, key=lambda x: x[1], reverse=True)


def extend_keywords_with_topics(lda_model, selected_topics, word_threshold=word_threshold, topn=5):
    extended_keywords = set()
    topics = dict(lda_model.show_topics(formatted=False, num_topics=lda_model.num_topics, num_words=topn))
    for topic_id, _, _ in selected_topics:
        topic_words = topics[topic_id]
        for word, score in topic_words:
            if score > word_threshold:
                extended_keywords.add(word)
    return extended_keywords


def preprocess_text(text):
    return " ".join(token.lemma_.lower() for token in nlp(text))


def preprocess_keywords(keywords):
    return ["_".join(token.lemma_.lower() for token in nlp(keyword)) for keyword in keywords]


def get_relevant_keywords(data):
    keywords = preprocess_keywords(data['keywords'])
    custom_topic_threshold = data.get('topic_threshold', topic_threshold)
    custom_word_threshold = data.get('word_threshold', word_threshold)
    relevant_topics = topic_relevance(lda, keywords, lda.num_topics, custom_topic_threshold)
    new_keywords = list(set([keyword.replace("_", " ") for keyword in
                             (keywords + list(
                                 extend_keywords_with_topics(lda, relevant_topics, custom_word_threshold)))]))
    return {"keywords": new_keywords}


def get_relevance_score(doc_topics, expected_keywords, lda_model, num_topics):
    relevant_topics = {}

    for keyword in expected_keywords:
        for topic_no, words in lda_model.show_topics(formatted=False, num_topics=num_topics):
            for word, prob in words:
                if word == keyword:
                    if topic_no in relevant_topics:
                        relevant_topics[topic_no] += prob
                    else:
                        relevant_topics[topic_no] = prob

    doc_score = 0
    for topic_id, weight in doc_topics:
        if topic_id in relevant_topics:
            doc_score += weight * relevant_topics[topic_id]

    return doc_score


def calculate_relevance_score(data):
    text = preprocess_text(data['text'])
    keywords = preprocess_keywords(data['keywords'])
    doc_bow = dictionary.doc2bow(preprocess_string(text))  # Convert document to bag-of-words format
    doc_topics = lda.get_document_topics(doc_bow)
    score = get_relevance_score(doc_topics, keywords, lda, lda.num_topics)
    return {"score": score}


class SimpleHTTPRequestHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Healthy")
        else:
            self.send_response(404)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b"Not Found")

    def do_POST(self):
        # Read the length of the content
        content_length = int(self.headers['Content-Length'])
        # Read the content
        post_data = self.rfile.read(content_length)

        # Attempt to parse JSON
        try:
            data = json.loads(post_data)
        except json.JSONDecodeError:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Bad request: Could not decode JSON")
            return

        if self.path == '/keywords':
            response = get_relevant_keywords(data)
        elif self.path == '/relevance':
            response = calculate_relevance_score(data)
        else:
            response = {
                'status': 'success',
                'message': 'POST data received, but no handler found',
                'yourData': data
            }

        # Send OK response
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

        self.wfile.write(json.dumps(response).encode('utf-8'))


def run(server_class=HTTPServer, handler_class=SimpleHTTPRequestHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f"Starting httpd server on port {port}")
    httpd.serve_forever()


if __name__ == "__main__":
    run()
