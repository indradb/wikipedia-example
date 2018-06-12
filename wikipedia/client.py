import json
import requests

# Server endpoint
ENDPOINT = "http://localhost:8000/graphql"

# How long in seconds before an IndraDB request times out
REQUEST_TIMEOUT = 600

# HTTP headers for the request
REQUEST_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}

class Variable:
    def __init__(self, name, value, type=None):
        self.name = name
        self.value = value
        self.type = type or "String!"

class Mutation:
    def __init__(self, name, *global_variables):
        self.name = name
        self.calls = []
        self.global_variables = global_variables

    def add(self, field, *local_variables):
        self.calls.append((field, local_variables))

    def request(self):
        if len(self.calls) == 0:
            return []

        calls = []
        variables = {v.name: v.value for v in self.global_variables}
        variable_types = {v.name: v.type for v in self.global_variables}

        for (i, (field, local_variables)) in enumerate(self.calls):
            for local_variable in local_variables:
                qualified_name = "_%s_%s" % (i, local_variable.name)
                field = field.replace("$%s" % local_variable.name, "$%s" % qualified_name)
                variables[qualified_name] = local_variable.value
                variable_types[qualified_name] = local_variable.type

            calls.append(field)

        query_typedefs = ", ".join("$%s: %s" % (name, t) for (name, t) in variable_types.items())
        query_body = "\n".join("_%s: %s" % (i, mutation) for (i, mutation) in enumerate(calls))
        query = """mutation %s(%s) {
            %s
        }""" % (self.name, query_typedefs, query_body)
        payload = dict(query=query, variables=variables)
        response = requests.post(ENDPOINT, data=json.dumps(payload), headers=REQUEST_HEADERS)
        data = response.json()["data"]
        return [data["_%s" % i] for i in range(len(calls))]

def create_vertices(t, count):
    mutation = Mutation("CreateManyVertices", Variable("t", t))

    for _ in range(count):
        mutation.add("createVertexFromType(t: $t)")

    return mutation.request()

def set_vertex_metadatas(metadata_name, values):
    mutation = Mutation("SetVertexMetadata", Variable("name", metadata_name))

    for (id, value) in values:
        mutation.add(
            """
            setMetadata(value: $value, q: {
                vertices: {
                    ids: [$id],
                    metadata: $name
                }
            })
            """,
            Variable("id", id, type="ID!"),
            Variable("value", json.dumps(value))
        )

        return mutation.request()

def create_edges(t, pairs):
    mutation = Mutation("CreateManyEdges", Variable("t", t))

    for (outbound_id, inbound_id) in pairs:
        mutation.add(
            "createEdge(key: { outboundId: $outboundId, t: $t, inboundId: $inboundId })",
            Variable("outboundId", outbound_id, type="ID!"),
            Variable("inboundId", inbound_id, type="ID!"),
        )

    return mutation.request()
