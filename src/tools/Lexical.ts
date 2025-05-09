import { YYTOKEN } from './SQLParser.js';
import { DFAAutomaton } from 'tslex/dist/automaton.js';
import lexicalRules from './lexicalRules.js';
export class Lexical {
  private dfa: DFAAutomaton;
  private finished = false;
  private lastToken?: YYTOKEN;
  constructor(src: string) {
    this.dfa = DFAAutomaton.deserialize(lexicalRules.rulesData);
    this.dfa.setSource(src);
    this.dfa.endHandler = () => {
      this.finished = true;
    };
  }
  yylex(): YYTOKEN {
    let genRet = (arg: YYTOKEN): YYTOKEN => {
      if (this.finished) {
        return {
          yytext: '',
          type: '$',
          value: '',
        };
      } else if (arg.type == 'space') {
        return this.yylex();
      } else {
        return arg;
      }
    };
    let ret = this.dfa.run(lexicalRules.handler) as YYTOKEN;
    this.lastToken = genRet(ret);
    return this.lastToken;
  }
  yyerror(msg: string) {
    console.error(`${msg}:${this.lastToken?.yytext}`);
  }
}
