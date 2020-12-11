#!/usr/bin/env python3

import uuid
import hashlib

from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler, HTTPError
from tornado.httpclient import HTTPClient
import indradb
import capnp

# Location of the templates
TEMPLATE_DIR = "./templates"

EDGE_LIMIT = 1000

def article_uuid(name):
    h = hashlib.blake2b(name.encode("utf8"), digest_size=16)
    return uuid.UUID(bytes=h.digest())

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
        # Get the ID of the article we want from its name
        article_id = article_uuid(article_name)

        # Get all of the data we want from IndraDB in a single
        # request/transaction
        vertex_query = indradb.SpecificVertexQuery(article_id)
        trans = self.client.transaction()

        vertex_data, edge_count, edge_data = capnp.join_promises([
            trans.get_vertices(vertex_query),
            trans.get_edge_count(article_id, None, "outbound"),
            trans.get_edges(vertex_query.outbound(EDGE_LIMIT).t("link")),
        ]).wait()

        if len(vertex_data) == 0:
            raise HTTPError(404)

        name_data = trans.get_vertex_properties(indradb.SpecificEdgeQuery(*[e.key for e in edge_data]).inbound(EDGE_LIMIT).property("name")).wait()
        
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
    client = indradb.Client("localhost:27615")
    app = Application([(r"/", HomeHandler, dict(client=client))], template_path=TEMPLATE_DIR)
    app.listen(8080)
    IOLoop.current().start()

if __name__ == "__main__":
    main()
