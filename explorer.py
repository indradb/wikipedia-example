#!/usr/bin/env python3

import wikipedia
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler, HTTPError
from tornado.httpclient import HTTPClient
import indradb
import json
import pickle
from urllib.parse import urlencode

# Location of the templates
TEMPLATE_DIR = "./templates"

# Query to use to get a vertex by ID
QUERY = """
query GetVertex($id: ID!) {
    query(q: {
        vertices: {
            ids: [$id]
        }
    }) {
        ... on OutputVertex {
            id,
            t
        }
    }
}
"""

class HomeHandler(RequestHandler):
    @classmethod
    def db(cls):
        if not hasattr(cls, "_db"):
            with open("data/article_names_to_ids.pickle", "rb") as f:
                cls._db = pickle.load(f)

        return cls._db

    def get(self):
        if self.get_argument("action", None) == "get_article":
            article_name = self.get_argument("article")
            article_id = self.db().get(article_name)

            if not article_id:
                raise HTTPError(404)

            self.redirect("http://localhost:27615/?%s" % urlencode({
                "query": QUERY,
                "variables": json.dumps({
                    "id": article_id
                })
            }))
        else:
            self.render("main.html")

def main():
    with wikipedia.server():
        app = Application([
            (r"/", HomeHandler),
        ], **{
            "template_path": TEMPLATE_DIR
        })

        app.listen(8080)
        IOLoop.current().start()

if __name__ == "__main__":
    main()
