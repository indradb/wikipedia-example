#!/usr/bin/env python3

import wikipedia
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler, HTTPError
from tornado.httpclient import HTTPClient
import indradb
import capnp

# Location of the templates
TEMPLATE_DIR = "./templates"

EDGE_LIMIT = 1000

class HomeHandler(RequestHandler):
    def initialize(self, client):
        self.client = client

    def get(self):
        if self.get_argument("action", None) == "get_article":
            article_name = self.get_argument("article")
            self.get_article(article_name)
        else:
            self.get_main()

    def get_article(self, article_name):
        vertex_query = indradb.SpecificVertexQuery(article_name)
        trans = self.client.transaction()

        vertex_data, edge_count, inbound_vertex_data = capnp.join_promises([
            trans.get_vertices(vertex_query),
            trans.get_edge_count(article_name, None, "outbound"),
            trans.get_vertices(vertex_query.outbound(EDGE_LIMIT).t("link").inbound(EDGE_LIMIT)),
        ]).wait()

        if len(vertex_data) == 0:
            raise HTTPError(404)

        self.render(
            "article.html",
            vertex=vertex_data[0],
            edge_count=edge_count,
            connected_vertices=inbound_vertex_data,
        )

    def get_main(self):
        self.render("main.html")

def main():
    with wikipedia.server() as client:
        handler_args = dict(client=client)
        app_settings = dict(template_path=TEMPLATE_DIR)
        app = Application([(r"/", HomeHandler, handler_args)], **app_settings)
        app.listen(8080)
        IOLoop.current().start()

if __name__ == "__main__":
    main()
