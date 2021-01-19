import json

from frocket.common.validation.path_visitor import PathVisitor

root = {
    "robot": False,
    "person": {
        "name": {
            "first": "elad", "last": "roz"
        },
        "primitives": [1, 2,
                       {"a": 3},
                       {"b": {"c": [4, 5, {"ping": "pong1"}]}},
                       {"b": {"c": [6, 7, {"ping": "pong2"}]}},
                       ],
        "nicks": [
            {"short": "e"},
            {"short": "roz"},
            {"short": None},
            {"long": {"heya": [123]}}
        ]
    }
}

original_root_json = json.dumps(root, sort_keys=True)


def root_clone():
    return json.loads(original_root_json)


def dict_equals_root(dct: dict):
    return json.dumps(dct, sort_keys=True) == original_root_json


def test_empty_root():
    assert PathVisitor(root={}, path="a.b.c").list() == []


def test_empty_path():
    assert PathVisitor(root=root, path="").list() == []
    assert PathVisitor(root=root, path="    ").list() == []


def test_robot():
    assert PathVisitor(root=root, path="robot").list() == [False]
    assert PathVisitor(root=root, path=" robot ").list() == [False]
    assert PathVisitor(root=root, path="robot.").list() == []


def test_toplevel_miss():
    assert PathVisitor(root=root, path="pers").list() == []
    assert PathVisitor(root=root, path=".pers.").list() == []


def test_person():
    assert PathVisitor(root=root, path="person").list() == [root['person']]
    assert PathVisitor(root=root, path="person.name").list() == [root['person']['name']]
    assert PathVisitor(root=root, path="person.name.first").list() == [root['person']['name']['first']]
    assert PathVisitor(root=root, path="person.name.last").list() == [root['person']['name']['last']]
    assert PathVisitor(root=root, path="person.name.last.").list() == []
    assert PathVisitor(root=root, path="person.name.lastx").list() == []
    assert PathVisitor(root=root, path="person.name.last.really.now").list() == []


def test_primitives():
    assert PathVisitor(root=root, path="person.primitives").list() == \
           root["person"]["primitives"]
    assert PathVisitor(root=root, path="person.primitives", list_to_items=False).list() == \
           [root["person"]["primitives"]]

    assert PathVisitor(root=root, path="person.primitives.").list() == []
    assert PathVisitor(root=root, path="person.primitives.", list_to_items=False).list() == []

    assert PathVisitor(root=root, path="person.primitives.a").list() == \
           [root["person"]["primitives"][2]["a"]]

    assert PathVisitor(root=root, path="person.primitives.b").list() == \
           [root["person"]["primitives"][3]["b"], root["person"]["primitives"][4]["b"]]

    # Should return a list of all elements in the matching two lists
    # noinspection PyUnresolvedReferences
    assert PathVisitor(root=root, path="person.primitives.b.c").list() == \
           [*root["person"]["primitives"][3]["b"]["c"], *root["person"]["primitives"][4]["b"]["c"]]

    # Should return a list with the matching two lists as they are
    # noinspection PyUnresolvedReferences
    assert PathVisitor(root=root, path="person.primitives.b.c", list_to_items=False).list() == \
           [root["person"]["primitives"][3]["b"]["c"], root["person"]["primitives"][4]["b"]["c"]]

    # These are two separate branches, to list_to_items should have no effect
    assert PathVisitor(root=root, path="person.primitives.b.c.ping").list() == \
           ["pong1", "pong2"]
    assert PathVisitor(root=root, path="person.primitives.b.c.ping", list_to_items=False).list() == \
           ["pong1", "pong2"]

    assert PathVisitor(root=root, path="person.primitives.b.c.ping.", list_to_items=False).list() == []
    assert PathVisitor(root=root, path="person.primitives.b.c.ping.lalala", list_to_items=False).list() == []


def test_nicks():
    assert PathVisitor(root=root, path="person.nicks.short").list() == \
           [root["person"]["nicks"][0]["short"],
            root["person"]["nicks"][1]["short"],
            root["person"]["nicks"][2]["short"]]

    assert PathVisitor(root=root, path="person.nicks.short.roz").list() == []

    assert PathVisitor(root=root, path="person.nicks.long").list() == \
           [root["person"]["nicks"][3]["long"]]

    # noinspection PyUnresolvedReferences
    assert PathVisitor(root=root, path="person.nicks.long.heya").list() == \
           [*root["person"]["nicks"][3]["long"]["heya"]]
    # noinspection PyUnresolvedReferences
    assert PathVisitor(root=root, path="person.nicks.long.heya", list_to_items=False).list() == \
           [root["person"]["nicks"][3]["long"]["heya"]]


def test_simple_visit():
    def collect(v):
        results.append(v)

    results = []
    d = root_clone()
    PathVisitor(root=d, path="person.name.last").visit(collect)
    PathVisitor(root=d, path="person.name.first").visit(collect)
    assert results == ["roz", "elad"]


def test_list_visit():
    def collect(v):
        results.append(v)

    # Also uses visit() twice on same object
    results = []
    p = PathVisitor(root=root, path="person.primitives.b.c")
    p.visit(collect)
    p.visit(collect)
    # noinspection PyUnresolvedReferences
    assert results == [*root["person"]["primitives"][3]["b"]["c"], *root["person"]["primitives"][4]["b"]["c"],
                       *root["person"]["primitives"][3]["b"]["c"], *root["person"]["primitives"][4]["b"]["c"]]


def test_dont_modify():
    d = root_clone()
    PathVisitor(root=d, path="person.nicks").visit(lambda v: "123")
    PathVisitor(root=d, path="person.name.first").visit(lambda v: "mosh")
    PathVisitor(root=d, path="person.name.last").visit(lambda v: False)
    assert dict_equals_root(d)


def test_simple_modify():
    d = root_clone()
    p = PathVisitor(root=d, path="person.name.last", modifiable=True)
    p.visit(lambda v: "newname")
    assert d['person']['name']['last'] == "newname"
    p.visit(lambda v: "roz")
    assert dict_equals_root(d)


def test_modify_complex():
    d = root_clone()
    wp = PathVisitor(root=d, path="person.name.last", modifiable=True)
    # Replace primitive with dict
    wp.visit(lambda v: {"a": "aaa", "b": "bbb"})
    assert PathVisitor(root=d, path="person.name.last.b").list() == ["bbb"]

    wp.visit(lambda v: "roz")
    assert dict_equals_root(d)  # After 1

    # Replace dict with dict
    wp.visit(lambda v: {"a2": "aaa2", "b2": "bbb2"})
    assert PathVisitor(root=d, path="person.name.last.b2").list() == ["bbb2"]
    assert PathVisitor(root=d, path="person.name.last.b").list() == []

    wp.visit(lambda v: "roz")
    assert dict_equals_root(d)  # After 2

    # Replace dict with list
    wp.visit(lambda v: ["a", "b", "c", {"d": "e"}])
    assert wp.list() == ["a", "b", "c", {"d": "e"}]
    assert PathVisitor(root=d, path="person.name.last", list_to_items=False).list() == [["a", "b", "c", {"d": "e"}]]
    assert PathVisitor(root=d, path="person.name.last.d").list() == ["e"]

    # Replace some list elements
    wp.visit(lambda v: "roz" if v in ["b", "c"] else None)
    assert PathVisitor(root=d, path="person.name.last").list() == ["a", "roz", "roz", {"d": "e"}]

    # Replace inner list to dict
    wp.visit(lambda v: ["d", "e"] if v == {"d": "e"} else None)
    assert PathVisitor(root=d, path="person.name.last").list() == ["a", "roz", "roz", ["d", "e"]]

    # ...and back
    wp.visit(lambda v: {"d", "e"} if v == ["d", "e"] else None)
    assert PathVisitor(root=d, path="person.name.last").list() == ["a", "roz", "roz", {"d", "e"}]

    # Now return from list to original primitive value. For that we don't want the visitor to iterate list items,
    # but return the list object
    PathVisitor(root=d, path="person.name.last", modifiable=True, list_to_items=False).\
        visit(lambda v: "roz" if isinstance(v, list) else "wtf")
    assert dict_equals_root(d)  # After 3
