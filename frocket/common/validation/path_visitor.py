from typing import Callable, Any, Optional

PathVisitorCallback = Callable[[Any], Optional[Any]]


class PathVisitor:
    """
    A helper class for safely fetching nested attributes in a dictionary.
    It is used extensively by the QueryValidator to extract and transform nested attributes.

    The class is instantiated with a root dict and a dot-delimited string path (e.g. 'attr.sub_attr.sub_sub').
    Then, visit() can be called once (or more) to run code over the matching value/s, if any. If the key is not found,
    no error is thrown. list() is a convenience method which visits the elements and returns them as a list,
    returning an empty list on no matches.

    By default, if the leaf key is a list, the visitor function is called for each element.
    However, if the list itself is what you need, pass list_to_items=False on init.

    Modifying attributes *below* the visited value is safe (be it a dict, a list, an object), however sometimes you
    may want to replace the whole value itself being itereated. For example, the QueryValidator replaces shorthand-
    notation objects, which are lists, into full-notation dicts.
    To support that, init the object with modifiable=true and return the replacement value from the visitor function,
    or None to keep the value.

    For usage examples, see test_path_visitor.py.
    """
    _KEY_NOT_FOUND = object()

    def __init__(self, root: dict, path: str, modifiable: bool = False, list_to_items: bool = True):
        assert (isinstance(root, dict))
        self._root = root
        self._paths = path.strip().split(".")
        self._modifiable = modifiable
        self._list_to_items = list_to_items

    def visit(self, func: PathVisitorCallback):
        if len(self._paths) > 0:
            self._visit_dict(self._root, 0, func)

    def list(self) -> list:
        result = []
        self.visit(lambda v: result.append(v))
        return result

    def _visit_dict(self, d: dict, depth: int, func: PathVisitorCallback):
        v = d.get(self._paths[depth], self._KEY_NOT_FOUND)  # Differentiate a None value from an inexisting key
        if v == self._KEY_NOT_FOUND:
            return  # Bumped into a wall

        if isinstance(v, list) and self._list_to_items:
            self._visit_list(v, depth + 1, func)
            return

        if depth == len(self._paths) - 1:
            replacement = func(v)  # Includes None
            if self._modifiable and replacement:
                d[self._paths[depth]] = replacement
        else:
            if not v:
                return
            elif isinstance(v, dict):
                self._visit_dict(v, depth + 1, func)
            elif isinstance(v, list):
                self._visit_list(v, depth + 1, func)
            else:
                return  # Can't go further

    def _visit_list(self, lst: list, depth: int, func: PathVisitorCallback):
        if depth == len(self._paths):
            assert self._list_to_items
            for i, elem in enumerate(lst):
                replacement = func(elem)
                if self._modifiable and replacement:
                    lst[i] = replacement
        else:
            for i, elem in enumerate(lst):
                # Note: depth is not incremented in this case, since elements are at the same 'path depth' as the list
                if isinstance(elem, dict):
                    self._visit_dict(elem, depth, func)
                elif isinstance(elem, list):
                    self._visit_list(elem, depth, func)
