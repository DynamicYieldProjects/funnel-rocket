"""
While the query schema is generally JSON-based (good for machines) rather then textual (like SQL,
supposedly human-friendly or at least more concise), there's one exception: an optional 'relation' expression allowing
to specify arbitrarily complex and/or relations between conditions, rather than just and/or over all.

The RelationParser class validates and breaks down the expression to a list of elements. However, it does not transform
them back into a Pandas query or similar - that is the query engine's responsibility and may change independently.

Note that conditions may be represented either by index ($0, $3, etc.) or by name - for named conditions.
"""
import logging
from typing import Type, List, Optional
from parsimonious.grammar import Grammar, NodeVisitor
from parsimonious.nodes import Node
from dataclasses import dataclass
from parsimonious.exceptions import ParseError, VisitationError
from abc import ABCMeta
from frocket.common.validation.consts import RELATION_OPS, map_condition_names, CONDITION_COLUMN_PREFIX
from frocket.common.validation.path_visitor import PathVisitor
from frocket.common.validation.error import ValidationErrorKind, QueryValidationError
from frocket.common.tasks.base import ErrorMessage

logger = logging.getLogger(__name__)

# TODO backlog fix the grammar to require whitespace between conditions and wordy-operators (and,or),
#  but not around symbol ops (&&, ||)
# TODO backlog fix "DeprecationWarning: invalid escape sequence \$"
RELATION_EXPRESSION_GRAMMAR = Grammar(
    """
    expression = (identifier / (open_paren ws? expression ws? close_paren)) (ws? op ws? expression)*
    identifier = condition_name / condition_id
    condition_name = ~r"\$[A-Z][A-Z_0-9]*"i
    condition_id = ~r"\$[0-9]+"
    op = "and" / "or" / "&&" / "||"
    ws = ~r"\s*"
    open_paren  = "("
    close_paren = ")"
    """)


@dataclass(frozen=True)
class RelationParserContext:
    condition_count: int
    named_conditions: dict
    column_prefix: str


@dataclass
class RBaseElement(metaclass=ABCMeta):
    text: str
    ctx: RelationParserContext
    condition_id: Optional[int] = None

    def validate(self) -> Optional[ErrorMessage]:
        pass

    def __str__(self):
        return f"{self.__class__.__name__}('{self.text}')"


@dataclass
class RTextElement(RBaseElement):
    pass


@dataclass
class RConditionBaseElement(RBaseElement):
    pass


@dataclass
class RConditionId(RConditionBaseElement):
    def validate(self):
        cid = int(self.text[1:])
        if cid >= self.ctx.condition_count:
            return f"Condition no. {cid} does not exist"
        self.condition_id = cid


@dataclass
class RConditionName(RConditionBaseElement):
    def validate(self):
        cname = self.text[1:]
        cid = self.ctx.named_conditions.get(cname, None)
        if cid is not None:  # Can be zero
            self.condition_id = cid
        else:
            return f"Condition named {self.text[1:]} does not exist"


@dataclass
class ROperator(RBaseElement):
    def validate(self):
        if self.text not in RELATION_OPS:
            return f"Operator {self.text} not in {RELATION_OPS}"


# noinspection PyMethodMayBeStatic,PyUnusedLocal
@dataclass
class RelationExpressionVisitor(NodeVisitor):
    """
    Used by the RelationParser to build the element list.
    Note that while the grammar is hierarchical, the resulting list isn't (no need, currently).
    """
    ctx: RelationParserContext

    def _build_element(self, node: Node, cls: Type[RBaseElement]):
        # noinspection PyArgumentList
        return cls(node.text, self.ctx)

    def visit_ws(self, node: Node, visited_children):
        return None  # Ignore whitespaces

    def visit_op(self, node: Node, visited_children):
        return self._build_element(node, ROperator)

    def visit_open_paren(self, node: Node, visited_children):
        return self._build_element(node, RTextElement)

    def visit_close_paren(self, node: Node, visited_children):
        return self._build_element(node, RTextElement)

    def visit_identifier(self, node: Node, visited_children):
        """Return the actual condition name / ID element (see grammar: identifier wraps conditions)."""
        return visited_children[0]

    def visit_condition_name(self, node: Node, visited_children):
        return self._build_element(node, RConditionName)

    def visit_condition_id(self, node: Node, visited_children):
        return self._build_element(node, RConditionId)

    def generic_visit(self, node: Node, visited_children):
        """Ignore current node, but return children (if any) as a flat list."""
        flat_result = []
        for child in visited_children:
            if type(child) is list:
                flat_result += child  # Unpack child array
            elif child:
                flat_result.append(child)
        return flat_result if len(flat_result) > 0 else None


class RelationParser:
    def __init__(self, query: dict):
        self._query = query
        self._condition_mapping = map_condition_names(query)
        self._used_conditions = None

        found_relations = PathVisitor(self._query, 'query.relation').list()
        assert len(found_relations) in [0, 1]
        self._relation = found_relations[0].strip().lower() if found_relations else None

    def parse(self) -> List[RBaseElement]:
        if not self._relation:
            return []

        ctx = RelationParserContext(condition_count=self._condition_mapping.count,
                                    named_conditions=self._condition_mapping.names,
                                    column_prefix=CONDITION_COLUMN_PREFIX)
        try:
            tree = RELATION_EXPRESSION_GRAMMAR.parse(self._relation)
        except ParseError as pe:
            # Adopted from within the ParseError class, but without the sometimes-confusing issue
            excerpt = pe.text[pe.pos:pe.pos + 20] if (pe.text and pe.pos is not None) else None
            if excerpt:
                message = f"Query relation is invalid around '{excerpt}' "
            else:
                message = f"Query relation '{self._relation}' is invalid"
            raise QueryValidationError(message, kind=ValidationErrorKind.RELATION)

        try:
            elements = RelationExpressionVisitor(ctx).visit(tree)
        except VisitationError as ve:
            logger.exception('Unexpected error while visiting parse tree')
            raise QueryValidationError(message=str(ve), kind=ValidationErrorKind.UNEXPECTED)

        for e in elements:
            error_message = e.validate()
            if error_message:
                raise QueryValidationError(message=error_message, kind=ValidationErrorKind.RELATION)

        self._used_conditions = [e.condition_id for e in elements if e.condition_id is not None]
        return elements

    @property
    def used_conditions(self) -> List[str]:
        return self._used_conditions
