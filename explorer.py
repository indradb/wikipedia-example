#!/usr/bin/env python3

import wikipedia
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler, HTTPError
from tornado.httpclient import HTTPClient
import indradb
import pickle

# Location of the templates
TEMPLATE_DIR = "./templates"

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
            self.get_article(article_name)
        else:
            self.get_main()

    def get_article(self, article_name):
        trans = indradb.Transaction()

        # Get the ID of the article we want from its name
        article_id = self.db()[article_name]

        if not article_id:
            raise HTTPError(404)

        # Get all of the data we want from IndraDB in a single
        # request/transaction
        vertex_query = indradb.VertexQuery.vertices([article_id])
        trans.get_vertices(vertex_query)
        trans.get_edge_count(article_id, None, "outbound")
        trans.get_edges(vertex_query.outbound_edges("link", limit=1000))
        trans.get_vertex_metadata(vertex_query.outbound_edges("link", limit=1000).inbound_vertices(), "name")
        trans.get_vertex_metadata(vertex_query, "eigenvector-centrality")
        client = wikipedia.get_client()
        [vertex_data, edge_count, edge_data, name_data, centrality_data] = client.transaction(trans)
        inbound_edge_ids = [e.key.inbound_id for e in edge_data]
        inbound_edge_names = {metadata.id: metadata.value for metadata in name_data}
        centrality = centrality_data[0].value if len(centrality_data) > 0 else None
        
        self.render(
            "article.html",
            article_name=article_name,
            article_id=article_id,
            vertex_data=vertex_data[0],
            edge_count=edge_count,
            inbound_edge_ids=inbound_edge_ids,
            inbound_edge_names=inbound_edge_names,
            centrality=centrality,
        )

    def get_main(self):
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
