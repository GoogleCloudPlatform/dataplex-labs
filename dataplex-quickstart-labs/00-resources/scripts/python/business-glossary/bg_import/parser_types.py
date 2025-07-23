"""Provides return type for csv parsers."""
from typing import Callable, TypeVar

import category as bg_category
import error
import term as bg_term

_ParseErrors = list[error.ParseError]
_ParserOutput = (
    dict[int, bg_category.Category] | dict[int, bg_term.Term]
)  # Dictionary mapping lines to categories | terms
_T = TypeVar("_T")
_ParseResult = tuple[_T, _ParseErrors]
_ParseFn = Callable[[str], _ParseResult[_T]]
_LinesRead = int
_ParserReturnType = tuple[_ParserOutput, _ParseErrors, _LinesRead]
