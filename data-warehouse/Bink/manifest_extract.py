# %%
import json
from typing import Dict, List, Optional
from enum import Enum

from pydantic import BaseModel, validator


class DbtResourceType(str, Enum):
    model = "model"
    analysis = "analysis"
    test = "test"
    operation = "operation"
    seed = "seed"
    source = "source"


class DbtMaterializationType(str, Enum):
    table = "table"
    view = "view"
    incremental = "incremental"
    ephemeral = "ephemeral"
    seed = "seed"
    test = "test"


class NodeDeps(BaseModel):
    nodes: List[str]


class NodeConfig(BaseModel):
    materialized: Optional[DbtMaterializationType]
    meta: Optional[Dict]


class Node(BaseModel):
    unique_id: str
    # path: Path
    resource_type: DbtResourceType
    description: str
    depends_on: Optional[NodeDeps]
    config: NodeConfig


class Manifest(BaseModel):
    nodes: Dict["str", Node]
    sources: Dict["str", Node]

    @validator("nodes", "sources")
    def filter(cls, val):
        return {k: v for k, v in val.items() if v.resource_type.value in ("test")}


if __name__ == "__main__":
    with open("target/manifest.json") as fh:
        data = json.load(fh)

    m = Manifest(**data)
# %% Get description for each node

test_meta_list = [
    {"test": node, "test_type": n.config.meta.get("test_type"), "description": n.config.meta.get("description")}
    for node, n in m.nodes.items()
]
full_test_list = [{"test": node, "config": n.config} for node, n in m.nodes.items()]
# %% Pull out unique tests and descriptions


def business_tests():
    output = set()
    for meta in test_meta_list:
        print(meta)
        if meta["test_type"] == "Business":
            header = "### " + str(meta["test"].split(".")[2]).split("__")[0] + "\n"
            description = str(meta["description"]) + "\n"
            markdown = (header, description)
            output.add(markdown)

    with open("markdown.txt", "w") as final:
        for x in output:
            final.write(x[0])
            final.write(x[1])


business_tests()
