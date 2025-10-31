from prompt_toolkit.shortcuts import prompt
from prompt_toolkit.styles import style_from_dict
from prompt_toolkit.token import Token
from prompt_toolkit.validation import Validator, ValidationError

class _OptionValidator(Validator):
    def __init__(self, options, default):
        super().__init__()
        self.options = [o.lower() for o in options]
        self.defaultAllowed = default is not None

    def validate(self, document):
        text = document.text.lower().strip()
        if self.defaultAllowed and not text:
            return
        elif text not in self.options:
            raise ValidationError(message='Invalid response', cursor_position=len(document.text))

class _StringValidator(Validator):
    def validate(self, document):
        text = document.text.strip()
        if not text:
            raise ValidationError(message='Invalid response', cursor_position=len(document.text))

_prompt_styles = style_from_dict({
    Token:          '#dddddd',
    Token.Sigil:    '#00ff00',
    Token.Prompt:   '#dddddd',
    Token.Symbol:   '#777777',
    Token.Option:   '#00ffff',
    Token.Default:  '#ff77ff',
})

def promptOptions(msg, options, default=None):
    tokens = [(Token.Sigil, "* "),
              (Token.Prompt, msg),
              (Token.Symbol, " ["),]

    first = True
    for option in options:
        if first:
            first = False
        else:
            tokens.append((Token.Symbol, ","))
        if option == default:
            tokens.append((Token.Default, option.upper()))
        else:
            tokens.append((Token.Option, option))

    tokens.append((Token.Symbol, "] : "))
    val = prompt(get_prompt_tokens=lambda x: tokens, style=_prompt_styles, validator=_OptionValidator(options, default))
    if val:
        return val.lower().strip()
    return default

def promptString(msg):
    tokens = [(Token.Sigil, "* "),
              (Token.Prompt, msg),
              (Token.Symbol, " : ")]
    val = prompt(get_prompt_tokens=lambda x: tokens, style=_prompt_styles, validator=_StringValidator())
    if val:
        return val.strip()
    return None
