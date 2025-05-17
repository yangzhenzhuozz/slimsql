import { genDFA } from 'tslex';
import { YYTOKEN } from '../tools/SQLParserDeclare.d.js';
import fs from 'fs';
let rules: {
  reg: string;
  handler: (text: string) => { yytext: string; type: string; value: any };
}[] = [
  {reg: '\\-\\-([^\n])*\n',handler: function (text) {return {yytext: text,type: 'space',value: text,};},}, //prettier-ignore
  {reg: '[ \t\n\r]+',handler: function (text) {return {yytext: text,type: 'space',value: text,};},}, //prettier-ignore

  {reg: 'partition',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'over',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore

  {reg: 'distinct',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'all',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore

  {reg: 'in',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'rlike',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'like',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore

  {reg: 'rows',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'row',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'range',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'between',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'unbounded',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'preceding',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'following',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'current',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore

  {reg: 'case',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'when',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'from',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'select',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'where',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'left',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'join',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'on',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: ',',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'as',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '<',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '<=',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '=',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '<>',handler: function (text) {return {yytext: text,type: '!=',value: text,};},}, // prettier-ignore
  {reg: '!=',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '>',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '>=',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '%',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '\\[',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '\\]',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore

  {reg: '\\*',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '\\.',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '\\+',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '\\-',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '\\*',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '/',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '\\(',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: '\\)',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'if',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'then',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'else',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'elseif',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'end',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'and',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'or',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'not',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'order',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'group',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'by',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'asc',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'desc',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'having',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'limit',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'is',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'null',handler: function (text) {return {yytext: text,type: text,value: null,};},}, // prettier-ignore
  {reg: 'cast',handler: function (text) {return {yytext: text,type: text,value: text,};},}, // prettier-ignore
  {reg: 'string',handler: function (text) {return {yytext: text,type: 'type',value: text,};},}, // prettier-ignore
  {reg: 'number',handler: function (text) {return {yytext: text,type: 'type',value: text,};},}, // prettier-ignore
  {reg: 'boolean',handler: function (text) {return {yytext: text,type: 'type',value: text,};},}, // prettier-ignore
  // prettier-ignore
  {reg: '[_a-zA-Z\\u4E00-\\u9FFF][a-zA-Z0-9_\\u4E00-\\u9FFF]*',handler: function (text) {return {yytext: text,type: 'id',value: text,};},}, //id的优先级最低,避免把关键字识别成id
  {
    reg: '`[^`]*`',
    handler: function (text) {
      let id = text.slice(1, -1);
      return { yytext: id, type: 'id', value: id };
    },
  }, //id的优先级最低,避免把关键字识别成id
  {
    reg: '[0-9]+\\.[0-9]+',
    handler: function (text) {
      return { yytext: text, type: 'number', value: Number(text) };
    },
  },
  {
    reg: '[0-9]+',
    handler: function (text) {
      return { yytext: text, type: 'number', value: Number(text) };
    },
  },
  {
    reg: `'(([^'\\\\])|(\\\\\\\\)|(\\\\')|(\\\\t)|(\\\\n)|(\\\\r)|(\\\\b)|(\\\\f))*'`,
    handler: function (text) {
      text = text.slice(1, -1); //去掉开头和结尾的单引号
      let str = '';
      let escape = false;
      for (let c of text) {
        if (escape) {
          escape = false;
          switch (c) {
            case 't':
              str += '\t';
              break;
            case 'n':
              str += '\n';
              break;
            case 'r':
              str += '\r';
              break;
            case '\\':
              str += '\\';
              break;
            case 'b':
              str += '\b';
              break;
            case 'f':
              str += '\f';
              break;
            case "'":
              str += "'";
              break;
            default:
              str += c;
              break;
          }
        } else if (c == '\\') {
          escape = true;
        } else {
          str += c;
        }
      }
      return { yytext: str, type: 'string', value: str };
    },
  },
];

let dfa = genDFA(rules.map((item) => item.reg));
let rulesData = dfa.serialize();

//用于在序列化的时候给函数参数加上string签名，这里是replace替换的，可能会有bug
let functionStrCache: { [key: string]: string } = {};
let serializedDfa = JSON.stringify(
  {
    rulesData,
    handler: rules.map((item) => item.handler),
  },
  (key, value) => {
    if (typeof value === 'function') {
      let signature = 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, function (c) {
        let r = (Math.random() * 16) | 0,
          v = c == 'x' ? r : (r & 0x3) | 0x8;
        return v.toString(16);
      });
      let functionStr = value.toString().replace(/^(function)?\s*\(([^\)]*)\)/, '$1 ($2:string)');
      functionStrCache[signature] = functionStr;
      return signature;
    } else {
      return value;
    }
  }
);

for (let k in functionStrCache) {
  serializedDfa = serializedDfa.replaceAll(`"${k}"`, functionStrCache[k]);
}
fs.writeFileSync('src/tools/lexicalRules.ts', `export default ${serializedDfa}`);
console.log('词法分析器生成成功');
