import json
from http.server import BaseHTTPRequestHandler, HTTPServer


def execute_correction(data):
    text = data['text']
    matches = data['matches']
    sorted_matches = sorted(matches, key=lambda x: x['offset'], reverse=True)

    for match in sorted_matches:
        if 'replacements' in match and type(match['replacements']) is list and len(match['replacements']) > 0:
            text = text[:match['offset']] + match['replacements'][0]['value'] + text[
                                                                                (match['offset'] + match['length']):]

    return {"text": text}


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

        if self.path == '/correction':
            response = execute_correction(data)
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
