#!/usr/bin/env python3

import wikipedia
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler, HTTPError
from tornado.httpclient import HTTPClient
import indradb
import shelve
import capnp

# Location of the templates
TEMPLATE_DIR = "./templates"

EDGE_LIMIT = 1000

class HomeHandler(RequestHandler):
    def initialize(self, client, db):
        self.client = client
        self.db = db

    def get(self):
        if self.get_argument("action", None) == "get_article":
            article_name = self.get_argument("article")
            self.get_article(article_name)
        else:
            self.get_main()

    def get_article(self, article_name):
        # Get the ID of the article we want from its name
        article_id = self.db[article_name]

        if not article_id:
            raise HTTPError(404)

        # Get all of the data we want from IndraDB in a single
        # request/transaction
        vertex_query = indradb.VertexQuery.vertices([article_id])
        trans = self.client.transaction()

        vertex_data, edge_count, edge_data = capnp.join_promises([
            trans.get_vertices(vertex_query),
            trans.get_edge_count(article_id, None, "outbound"),
            trans.get_edges(vertex_query.outbound_edges(EDGE_LIMIT, type_filter="link")),
        ]).wait()

        name_data = trans.get_vertex_properties(indradb.EdgeQuery.edges([e.key for e in edge_data]).inbound_vertices(EDGE_LIMIT), "name").wait()
        
        inbound_edge_ids = [e.key.inbound_id for e in edge_data]
        inbound_edge_names = {p.id: p.value for p in name_data}

        self.render(
            "article.html",
            article_name=article_name,
            article_id=article_id,
            vertex_data=vertex_data[0],
            edge_count=edge_count,
            inbound_edge_ids=inbound_edge_ids,
            inbound_edge_names=inbound_edge_names,
        )

    def get_main(self):
        self.render("main.html")

def main():
    with wikipedia.server() as client:
        with shelve.open("data/article_names_to_ids.shelve") as db:
            handler_args = dict(client=client, db=db)
            app_settings = dict(template_path=TEMPLATE_DIR)
            app = Application([(r"/", HomeHandler, handler_args)], **app_settings)
            app.listen(8080)
            IOLoop.current().start()

if __name__ == "__main__":
    main()
