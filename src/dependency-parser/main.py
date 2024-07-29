from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from socketserver import BaseServer
from spacy.tokens import Doc, Token

import spacy


def convert_token_to_data(token: Token):
    return {
        "value": token.text,
        "index": token.i,
        "dependency": token.dep_,
        "parentIndex": token.head.i
    }


nlp = spacy.load("uk_core_news_lg")
print('initialized nlp')


def do_dependency_parsing(data):

    resultSentences = []

    sentences = [[token['value'] for token in sentence['tokens']] for sentence in data['sentences']]

    for sentence in sentences:
        doc = Doc(nlp.vocab, sentence)
        resultSentences.append({"tokens": [convert_token_to_data(token) for token in nlp(doc)]})

    return {"sentences": resultSentences}


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

        if self.path == '/dependency-parsing':
            response = do_dependency_parsing(data)
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
