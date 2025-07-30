import fs from 'fs';
import { Grammar, default as TSCC } from 'tscc-lr1';
import { ExpNode, FrameOffset, FrameRange, SelectClause, WindowFunction, Row } from '../tools/ExpTree.js';
import { assert } from '../tools/assert.js';
declare type UDFHanler =
  | {
      type: 'normal';
      handler: (...args: any[]) => any | undefined;
    }
  | {
      type: 'aggregate';
      handler: (list: any[][], modifier?: 'distinct' | 'all') => any | undefined;
    }
  | {
      type: 'windowFrame';
      handler: (list: any[][], frameInfo: WindowFunction) => any[] | undefined;
    };
declare type UDF = {
  [key: string]: UDFHanler;
};
declare class SQLContext {
  intermediatView: {
    [key: string]: {
      data: Row[];
      fields: Set<string>;
    };
  };
  directFieldT: { [key: string]: string };
  constructor(udf: UDF);
  addTV(
    view: {
      data: Row[];
      fields: Set<string>;
    },
    name: string
  ): void;
  alias(table: string, alias: string): void;
  limit(exp: ExpNode): void;
  groupBy(exps: ExpNode[], groupType: 'frame' | 'group'): void;
  where(exps: ExpNode): void;
  select(select_clause: SelectClause, orderClause?: ExpNode[]): { data: Row[]; fields: Set<string> };
  leftJoin(lefts: string[], right: string, exp: ExpNode): string[];
}

//暂时只需要用到SQLSession的tableView属性
declare interface SQLSession {
  tableView: {
    [key: string]: {
      data: Row[];
      fields: Set<string>;
    };
  };
  udf: UDF;
}

declare let Context: {
  session: SQLSession;
  ctx: SQLContext;
  ctxStack: SQLContext[]; //每次select的时候都先创建一个ctx
};
function gen() {
  let grammar: Grammar = {
    userCode: `//这个文件用SQLParserGen.ts生成的\nimport { assert } from './assert.js';\nimport { SQLContext } from './SQLSession.js';\n\n\nlet ctxStack=[] as SQLContext[];//每次select的时候都先创建一个ctx\n\n`,
    tokens: [
      '.',
      'partition',
      'over',
      'left',
      'join',
      'from',
      'on',
      'id',
      'select',
      'where',
      ',',
      'as',
      'is',
      'null',
      'case',
      'when',
      '<',
      '<=',
      '>',
      '>=',
      '=',
      '+',
      '-',
      '*',
      '/',
      '%',
      '(',
      ')',
      '[',
      ']',
      'if',
      'then',
      'else',
      'elseif',
      'end',
      'and',
      'or',
      'not',
      'order',
      'group',
      'by',
      'asc',
      'desc',
      'having',
      'limit',
      'number',
      'string',
      'cast',
      'type',
      'distinct',
      'all',
      'rows',
      'range',
      'between',
      'unbounded',
      'preceding',
      'following',
      'current',
      'row',
    ],
    association: [
      { left: ['or'] },
      { left: ['and'] },
      { left: ['not'] },
      { left: ['is', '<', '<=', '=', '>', '>=', '!=', 'rlike', 'like', 'in'] },
      { left: ['%'] },
      { left: ['+', '-'] },
      { left: ['*', '/'] },
      { left: ['['] },
      { nonassoc: ['low_as'] }, //比as优先级低一些
      { nonassoc: ['as'] },
    ],
    accept: function ($) {
      return $[0] as { data: Row[]; fields: Set<string> };
    },
    BNF: [
      {
        'program:query': {
          action: function ($) {
            return $[0] as { data: Row[]; fields: Set<string> };
          },
        },
      },
      {
        'query:before_select select_clause from tableView where_clause group_clause order_clause limit_clause': {
          action: function ($): { data: Row[]; fields: Set<string> } {
            let select_clause = $[1] as SelectClause;
            let where_clause = $[4] as ExpNode | undefined;
            let group_clause = $[5] as ExpNode | undefined;
            let order_clause = $[6] as ExpNode[] | undefined;
            let limit_clause = $[7] as ExpNode | undefined;

            if (where_clause) {
              Context.ctx.where(where_clause);
            }
            if (group_clause != undefined) {
              if (group_clause.op == 'group') {
                Context.ctx.groupBy(group_clause.children!, 'group');
              } else if (group_clause.op == 'group_having') {
                let group_exps = group_clause.children!.slice(0, -1);
                let having_condition = group_clause.children!.slice(-1)[0];
                Context.ctx.groupBy(group_exps, 'group');
                Context.ctx.where(having_condition);
              }
            }

            if (limit_clause != undefined) {
              Context.ctx.limit(limit_clause);
            }

            for (let i = 0; i < select_clause.nodes.length; i++) {
              let node = select_clause.nodes[i];
              if (node.op == '*') {
                let any_nodes = [] as ExpNode[];
                for (let tn in Context.ctx.intermediatView)
                  for (let k of Context.ctx.intermediatView[tn].fields) {
                    if (Context.ctx.directFieldT[k] != undefined) {
                      any_nodes.push({
                        op: 'getfield',
                        value: k,
                        targetName: k,
                      });
                    } else {
                      any_nodes.push({
                        op: 'getTableField',
                        value: `${tn}.${k}`,
                        targetName: `${tn}.${k}`,
                      });
                    }
                  }
                select_clause.nodes = [...select_clause.nodes.slice(0, i), ...any_nodes, ...select_clause.nodes.slice(i + 1)];
              }
            }

            let ret = Context.ctx.select(select_clause, order_clause);
            Context.ctx = Context.ctxStack.pop()!;
            return ret;
          },
        },
      },
      {
        'before_select:': {
          action: function () {
            Context.ctxStack.push(Context.ctx);
            Context.ctx = new SQLContext(Context.session.udf);
          },
        },
      },
      {
        'select_clause:select modifier select_list': {
          action: function ($): SelectClause {
            return {
              modifier: $[1] as 'distinct' | 'all' | undefined,
              nodes: $[2] as ExpNode[],
            };
          },
        },
      },
      {
        'select_list:select_list , select_item': {
          action: function ($): ExpNode[] {
            let select_list = $[0] as ExpNode[];
            let select_item = $[2] as ExpNode;
            select_list.push(select_item);
            return select_list;
          },
        },
      },
      {
        'select_list:select_item': {
          action: function ($): ExpNode[] {
            return [$[0] as ExpNode];
          },
        },
      },
      {
        'select_item:exp': {
          action: function ($): ExpNode {
            return $[0] as ExpNode;
          },
        },
      },
      {
        'select_item:windowFunction': {
          action: function ($): ExpNode {
            return $[0] as ExpNode;
          },
        },
      },
      {
        'select_item:*': {
          action: function ($): ExpNode {
            return {
              op: '*',
              targetName: '*',
            };
          },
        },
      },
      {
        'frame_clause:': {
          action: function (): FrameRange {
            return {
              start: {
                offset: 'unbounded',
                type: 'preceding',
              },
              end: {
                offset: 'unbounded',
                type: 'following',
              },
            };
          },
        },
      },
      {
        'frame_clause:rows between frame_offset and frame_offset': {
          action: function ($): FrameRange {
            let start = $[2] as FrameOffset;
            let end = $[4] as FrameOffset;
            return {
              start,
              end,
            };
          },
        },
      },
      {
        'frame_offset:number preceding': {
          action: function ($): FrameOffset {
            return {
              offset: $[0] as number,
              type: 'preceding',
            };
          },
        },
      },
      {
        'frame_offset:number following': {
          action: function ($): FrameOffset {
            return {
              offset: $[0] as number,
              type: 'following',
            };
          },
        },
      },
      {
        'frame_offset:unbounded preceding': {
          action: function ($): FrameOffset {
            return {
              offset: 'unbounded',
              type: 'preceding',
            };
          },
        },
      },
      {
        'frame_offset:unbounded following': {
          action: function ($): FrameOffset {
            return {
              offset: 'unbounded',
              type: 'following',
            };
          },
        },
      },
      {
        'frame_offset:current row': {
          action: function ($): FrameOffset {
            return {
              offset: 0,
              type: 'current row',
            };
          },
        },
      },
      {
        'partition_clause:': {
          action: function ($): ExpNode[] {
            return [
              {
                op: 'immediate_val',
                value: 1,
                targetName: '1',
              },
            ];
          },
        },
      }, //默认创建一个exp=1的计算列,因为一定需要分区才能开窗
      {
        'partition_clause:partition by partition_list': {
          action: function ($): ExpNode[] {
            return $[2] as ExpNode[];
          },
        },
      },
      {
        'partition_list:partition_list , partition_item': {
          action: function ($): ExpNode[] {
            let partition_list = $[0] as ExpNode[];
            let partition_item = $[2] as ExpNode;
            return [...partition_list, partition_item];
          },
        },
      },
      {
        'partition_list:partition_item': {
          action: function ($): ExpNode[] {
            return [$[0]] as ExpNode[];
          },
        },
      },
      {
        'partition_item:exp': {
          action: function ($): ExpNode {
            return $[0] as ExpNode;
          },
        },
      },
      {
        'select_item:alias_exp': {
          action: function ($) {
            return $[0] as ExpNode;
          },
        },
      },
      {
        'alias_exp:exp as id': {
          action: function ($): ExpNode {
            let exp = $[0] as ExpNode;
            let id = $[2] as string;
            return {
              op: 'alias',
              targetName: id, //有alias时，强制改名
              children: [exp],
            };
          },
        },
      },
      {
        'tableView:id': {
          action: function ($): string[] {
            let id = $[0] as string;
            if (Context.ctx.intermediatView[id] === undefined) {
              if (Context.session.tableView[id] === undefined) {
                throw `表"${id}"不存在`;
              } else {
                let clone = {
                  data: Context.session.tableView[id].data.slice(),
                  fields: Context.session.tableView[id].fields,
                };
                Context.ctx.addTV(clone, id);
              }
            }
            return [id];
          },
        },
      },
      {
        'tableView:id as id': {
          action: function ($): string[] {
            let id = $[0] as string;
            let alias = $[2] as string;
            if (Context.ctx.intermediatView[id] === undefined) {
              if (Context.session.tableView[id] === undefined) {
                throw `表"${id}"不存在`;
              } else {
                let clone = {
                  data: Context.session.tableView[id].data.slice(),
                  fields: Context.session.tableView[id].fields,
                };
                Context.ctx.addTV(clone, id);
              }
            }
            Context.ctx.alias(id, alias);
            return [alias];
          },
        },
      },
      {
        'tableView:( query ) as  id': {
          action: function ($): string[] {
            let query = $[1] as { data: Row[]; fields: Set<string> };
            let id = $[4] as string;
            Context.ctx.addTV(
              {
                data: query.data.slice(),
                fields: query.fields,
              },
              id
            );
            return [id];
          },
        },
      },
      {
        'tableView:tableView left join tableView on exp': {
          action: function ($): string[] {
            let lefts = $[0] as string[];
            let right = $[3] as string[];
            assert(Array.isArray(right) && right.length == 1);
            let condition = $[5] as ExpNode;
            return Context.ctx.leftJoin(lefts, right[0], condition);
          },
        },
      },
      {
        'where_clause:': {
          action: function () {
            //什么也不做
          },
        },
      },
      {
        'where_clause:where where_condition': {
          action: function ($, stack): ExpNode {
            return $[1] as ExpNode;
          },
        },
      },
      {
        'where_condition:exp': {
          action: function ($): ExpNode {
            return $[0] as ExpNode;
          },
        },
      },
      {
        'group_clause:': {
          action: function () {
            //什么也不做
          },
        },
      },
      {
        'group_clause:group by group_list having_clause': {
          action: function ($): ExpNode {
            let group_list = $[2] as ExpNode[];
            let having_clause = $[3] as ExpNode | undefined;
            let targetName = '';
            for (let i = 0; i < group_list.length; i++) {
              if (i == 0) {
                targetName += group_list[i].targetName;
              } else {
                targetName += ',' + group_list[i].targetName;
              }
            }
            if (having_clause != undefined) {
              targetName += 'having ' + having_clause?.targetName;
              return {
                op: 'group_having',
                children: [...group_list, having_clause],
                targetName: targetName,
              };
            } else {
              return {
                op: 'group',
                children: [...group_list],
                targetName: targetName,
              };
            }
          },
        },
      },
      {
        'group_list:group_list , group_item': {
          action: function ($): ExpNode[] {
            let group_list = $[0] as ExpNode[];
            let group_item = $[2] as ExpNode;
            return [...group_list, group_item];
          },
        },
      },
      {
        'group_list:group_item': {
          action: function ($): ExpNode[] {
            let group_item = $[0] as ExpNode;
            return [group_item];
          },
        },
      },
      {
        'group_item:exp': {
          action: function ($): ExpNode {
            let group_item = $[0] as ExpNode;
            return group_item;
          },
        },
      }, //这里可能会导致性能问题，如:from t group by f1(t.v1) select f1(t.v1)，甚至是非常复杂的表达式计算，实在没时间做优化了
      {
        'having_clause:': {
          action: function () {
            //什么也不做
          },
        },
      },
      {
        'having_clause:having exp': {
          action: function ($) {
            let exp = $[1] as ExpNode;
            return exp;
          },
        },
      },
      {
        'order_clause:': {
          action: function () {
            //什么也不做
          },
        },
      },
      {
        'order_clause:order by order_by_list': {
          action: function ($): ExpNode[] {
            let order_by_list = $[2] as ExpNode[];
            return order_by_list;
          },
        },
      },
      {
        'order_by_list:order_by_list , order_by_item': {
          action: function ($): ExpNode[] {
            let order_by_list = $[0] as ExpNode[];
            let order_by_item = $[2] as ExpNode;
            return [...order_by_list, order_by_item];
          },
        },
      },
      {
        'order_by_list:order_by_item': {
          action: function ($): ExpNode[] {
            return [$[0] as ExpNode];
          },
        },
      },
      {
        'order_by_item:exp order_key': {
          action: function ($): ExpNode {
            return {
              op: 'order',
              children: [$[0] as ExpNode],
              order: $[1] as 'asc' | 'desc',
              targetName: `${(<ExpNode>$[0]).targetName}`,
            };
          },
        },
      },
      {
        'order_by_item:baseWindowFunction order_key': {
          action: function ($): ExpNode {
            return {
              op: 'order',
              children: [
                {
                  op: 'windowFunction',
                  windowFunction: $[0] as WindowFunction,
                  targetName: $[0].targetName,
                },
              ],
              order: $[1] as 'asc' | 'desc',
              targetName: `${(<ExpNode>$[0]).targetName}`,
            };
          },
        },
      },
      {
        'order_key:': {
          action: function () {
            //如果不指定排序key,则默认为asc
            return 'asc';
          },
        },
      },
      {
        'order_key:asc': {
          action: function () {
            return 'asc';
          },
        },
      },
      {
        'order_key:desc': {
          action: function () {
            return 'desc';
          },
        },
      },
      {
        'limit_clause:': {
          action: function () {
            //什么也不做
          },
        },
      },
      {
        'limit_clause:limit number': {
          action: function ($): ExpNode {
            let n = $[1] as number;
            return {
              op: 'limit',
              targetName: `limit ${n}`,
              limit: [n],
            };
          },
        },
      },
      {
        'limit_clause:limit number , number': {
          action: function ($): ExpNode {
            let n1 = $[1] as number;
            let n2 = $[3] as number;
            return {
              op: 'limit',
              targetName: `limit ${n1},${n2}`,
              limit: [n1, n2],
            };
          },
        },
      },
      {
        'exp:number': {
          action: function ($): ExpNode {
            let n = $[0] as number;
            return {
              op: 'immediate_val',
              value: n,
              targetName: n.toString(),
            };
          },
        },
      },
      {
        'exp:string': {
          action: function ($): ExpNode {
            let n = $[0] as string;
            return {
              op: 'immediate_val',
              value: n,
              targetName: `${JSON.stringify(n)}`,
            };
          },
        },
      },
      {
        'exp:id . id': {
          action: function ($): ExpNode {
            let tableName = $[0] as string;
            let fieldName = $[2] as string;
            return {
              op: 'getTableField',
              value: `${tableName}.${fieldName}`,
              targetName: `${tableName}.${fieldName}`,
            };
          },
        },
      },
      {
        'exp:id': {
          action: function ($): ExpNode {
            let fieldName = $[0] as string;
            return {
              op: 'getfield',
              value: `${fieldName}`,
              targetName: fieldName,
            };
          },
        },
      },
      {
        'exp:exp % exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'mod',
              children: [e1, e2],
              targetName: `${e1.targetName} % ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp + exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'add',
              children: [e1, e2],
              targetName: `${e1.targetName} + ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp [ exp ]': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'index',
              children: [e1, e2],
              targetName: `${e1.targetName} [${e2.targetName}]`,
            };
          },
        },
      },
      {
        'exp:exp - exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'sub',
              children: [e1, e2],
              targetName: `${e1.targetName} - ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp * exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'mul',
              children: [e1, e2],
              targetName: `${e1.targetName} * ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp < exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'lt',
              children: [e1, e2],
              targetName: `${e1.targetName} < ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp <= exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'le',
              children: [e1, e2],
              targetName: `${e1.targetName} <= ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp > exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'gt',
              children: [e1, e2],
              targetName: `${e1.targetName} > ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp >= exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'ge',
              children: [e1, e2],
              targetName: `${e1.targetName} >= ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp = exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'eq',
              children: [e1, e2],
              targetName: `${e1.targetName} = ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp != exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'ne',
              children: [e1, e2],
              targetName: `${e1.targetName} = ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp / exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'div',
              children: [e1, e2],
              targetName: `${e1.targetName} / ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:( exp )': {
          action: function ($): ExpNode {
            let exp = $[1] as ExpNode;
            exp.targetName = `(${exp.targetName})`;
            return exp;
          },
        },
      },
      {
        'exp:exp and exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'and',
              children: [e1, e2],
              targetName: `${e1.targetName} and ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp or exp': {
          action: function ($): ExpNode {
            let e1 = $[0] as ExpNode;
            let e2 = $[2] as ExpNode;
            return {
              op: 'or',
              children: [e1, e2],
              targetName: `${e1.targetName} or ${e2.targetName}`,
            };
          },
        },
      },
      {
        'exp:not exp': {
          action: function ($): ExpNode {
            let exp = $[1] as ExpNode;
            return {
              op: 'not',
              children: [exp],
              targetName: `not ${exp.targetName}`,
            };
          },
        },
      },
      {
        'exp:exp is null': {
          action: function ($): ExpNode {
            let exp = $[0] as ExpNode;
            return {
              op: 'is_null',
              children: [exp],
              targetName: `${exp.targetName} is null`,
            };
          },
        },
      },
      {
        'exp:exp is not null': {
          action: function ($): ExpNode {
            let exp = $[0] as ExpNode;
            return {
              op: 'is_not_null',
              children: [exp],
              targetName: `${exp.targetName} is not null`,
            };
          },
        },
      },
      {
        'exp:cast ( exp as type )': {
          action: function ($): ExpNode {
            let exp = $[2] as ExpNode;
            let type = $[4] as 'number' | 'string' | 'boolean';
            return {
              op: 'cast',
              children: [exp],
              cast_type: type,
              targetName: `cast (${exp.targetName} as ${type})`,
            };
          },
        },
      },
      {
        'exp:case when_list else_clause end': {
          action: function ($): ExpNode {
            let when_listTargetName = '';
            for (let item of $[1] as ExpNode[]) {
              when_listTargetName += item.targetName + '\n';
            }
            if ($[2] != undefined) {
              when_listTargetName += ($[2] as ExpNode).targetName + '\n';
            }
            return {
              op: 'case',
              children: [...($[1] as ExpNode[]), $[2] as ExpNode],
              targetName: `case ${when_listTargetName} end`,
            };
          },
        },
      },
      {
        'exp:case exp when_list else_clause end': {
          action: function ($): ExpNode {
            let when_listTargetName = ($[1] as ExpNode).targetName + '\n';
            for (let item of $[2] as ExpNode[]) {
              when_listTargetName += item.targetName + '\n';
            }
            if ($[3] != undefined) {
              when_listTargetName += ($[3] as ExpNode).targetName + '\n';
            }
            return {
              op: 'case-exp',
              children: [$[1] as ExpNode, ...($[2] as ExpNode[]), $[3] as ExpNode],
              targetName: `case ${when_listTargetName} end`,
            };
          },
        },
      },
      {
        'when_list:when_list when_clause': {
          action: function ($): ExpNode[] {
            let when_list = $[0] as ExpNode[];
            let when_clause = $[1] as ExpNode;
            return [...when_list, when_clause];
          },
        },
      },
      {
        'when_list:when_clause': {
          action: function ($): ExpNode[] {
            return [$[0] as ExpNode];
          },
        },
      },
      {
        'when_clause:when exp then exp': {
          action: function ($): ExpNode {
            return {
              op: 'when',
              children: [$[1] as ExpNode, $[3] as ExpNode],
              targetName: `when ${$[1].targetName} then ${$[3].targetName}`,
            };
          },
        },
      },
      {
        'else_clause:': {},
      },
      {
        'else_clause:else exp': {
          action: function ($): ExpNode {
            return {
              op: 'else',
              children: [$[1] as ExpNode],
              targetName: `else ${$[1].targetName}`,
            };
          },
        },
      },
      {
        'exp:call': {
          action: function ($): ExpNode {
            return $[0] as ExpNode;
          },
        },
      },
      {
        'exp:null': {
          action: function ($): ExpNode {
            return {
              op: 'immediate_val',
              value: null,
              targetName: 'null',
            };
          },
        },
      },
      {
        'exp:exp rlike exp': {
          action: function ($): ExpNode {
            return {
              op: 'rlike',
              children: [$[0] as ExpNode, $[2] as ExpNode],
              targetName: `${($[0] as ExpNode).targetName} rlike ${($[2] as ExpNode).targetName}`,
            };
          },
        },
      },
      {
        'exp:exp not rlike exp': {
          action: function ($): ExpNode {
            return {
              op: 'not-rlike',
              children: [$[0] as ExpNode, $[3] as ExpNode],
              targetName: `${($[0] as ExpNode).targetName} not rlike ${($[3] as ExpNode).targetName}`,
            };
          },
        },
      },
      {
        'exp:exp like exp': {
          action: function ($): ExpNode {
            return {
              op: 'like',
              children: [$[0] as ExpNode, $[2] as ExpNode],
              targetName: `${($[0] as ExpNode).targetName} like ${($[2] as ExpNode).targetName}`,
            };
          },
        },
      },
      {
        'exp:exp not like exp': {
          action: function ($): ExpNode {
            return {
              op: 'not-like',
              children: [$[0] as ExpNode, $[3] as ExpNode],
              targetName: `${($[0] as ExpNode).targetName} not like ${($[3] as ExpNode).targetName}`,
            };
          },
        },
      },
      {
        'windowFunction:baseWindowFunction': {
          action: function ($): ExpNode {
            let windowFunction = $[0] as WindowFunction;
            return {
              op: 'windowFunction',
              windowFunction: windowFunction,
              targetName: windowFunction.targetName,
            };
          },
        },
      },
      {
        'windowFunction:baseWindowFunction as id': {
          action: function ($): ExpNode {
            let windowFunction = $[0] as WindowFunction;
            windowFunction.alias = $[2] as string;
            return {
              op: 'windowFunction',
              windowFunction: windowFunction,
              targetName: windowFunction.targetName,
            };
          },
        },
      },
      {
        'baseWindowFunction:call over ( partition_clause order_clause frame_clause )': {
          action: function ($): WindowFunction {
            let windowFunction = $[0] as ExpNode;
            let partition = $[3] as ExpNode[];
            let order = $[4] as ExpNode[] | undefined;
            let partionName = '';
            for (let i = 0; i < partition.length; i++) {
              if (i == 0) {
                partionName += partition[i].targetName;
              } else {
                partionName += ',' + partition[i].targetName;
              }
            }
            let orderName = '';
            for (let i = 0; order !== undefined && i < order.length; i++) {
              if (i == 0) {
                orderName += `${order[i].targetName} ${order[i].order}`;
              } else {
                orderName += `,${order[i].targetName} ${order[i].order}`;
              }
            }
            let frameRangeName = '';
            let frameRange = $[5] as FrameRange;
            if ($[5] != undefined) {
              let startName = frameRange.start.type == 'current row' ? 'current row' : `${frameRange.start.offset} ${frameRange.start.type}`;
              let endName = frameRange.end.type == 'current row' ? 'current row' : `${frameRange.end.offset} ${frameRange.end.type}`;
              frameRangeName = `rows between ${startName} and ${endName}`;
            }
            return {
              windowFunction,
              partition,
              order,
              frameRange: $[5] as FrameRange,
              targetName: `${windowFunction.targetName} over (partition by ${partionName} ${orderName != '' ? 'order by ' + orderName : ''} ${frameRangeName})`,
            };
          },
        },
      },
      {
        'exp:exp not in ( in_list )': {
          action: function ($): ExpNode {
            let listName = '';
            let listItem = $[4] as ExpNode[];
            for (let i = 0; i < listItem.length; i++) {
              if (i == 0) {
                listName += listItem[i].targetName;
              } else {
                listName += ',' + listItem[i].targetName;
              }
            }
            return {
              op: 'not-in',
              children: [$[0] as ExpNode, ...($[4] as ExpNode[])],
              targetName: `${$[0].targetName} not in (${listName})`,
            };
          },
        },
      },
      {
        'exp:exp in ( in_list )': {
          action: function ($): ExpNode {
            let listName = '';
            let listItem = $[3] as ExpNode[];
            for (let i = 0; i < listItem.length; i++) {
              if (i == 0) {
                listName += listItem[i].targetName;
              } else {
                listName += ',' + listItem[i].targetName;
              }
            }
            return {
              op: 'in',
              children: [$[0] as ExpNode, ...($[3] as ExpNode[])],
              targetName: `${$[0].targetName} in (${listName})`,
            };
          },
        },
      },
      {
        'in_list:in_list , exp': {
          action: function ($): ExpNode[] {
            let list = $[0] as ExpNode[];
            let exp = $[2] as ExpNode;
            list.push(exp);
            return list;
          },
        },
      },
      {
        'in_list:exp': {
          action: function ($): ExpNode[] {
            return [$[0] as ExpNode];
          },
        },
      },
      {
        'call:id ( modifier argu_list )': {
          action: function ($): ExpNode {
            let id = $[0] as string;
            let modifier = $[2] as 'distinct' | 'all' | undefined;
            let args = $[3] as ExpNode[];
            let argNames = '';
            for (let i = 0; i < args.length; i++) {
              if (i == 0) {
                argNames += args[i].targetName;
              } else {
                argNames += ',' + args[i].targetName;
              }
            }
            return {
              op: 'call',
              value: id,
              children: args,
              modifier: modifier,
              targetName: `${id}(${argNames})`,
            };
          },
        },
      },
      {
        'argu_list:ε': {
          action: function ($): ExpNode[] {
            return [];
          },
        },
      },
      {
        'argu_list:argu_list , exp': {
          action: function ($): ExpNode[] {
            let argu_list = $[0] as ExpNode[];
            let exp = $[2] as ExpNode;
            return [...argu_list, exp];
          },
        },
      },
      {
        'argu_list:exp': {
          action: function ($): ExpNode[] {
            return [$[0] as ExpNode];
          },
        },
      },
      {
        'modifier:': {},
      },
      {
        'modifier:distinct': {
          action: function () {
            return 'distinct';
          },
        },
      },
      {
        'modifier:all': {
          action: function () {
            return 'all';
          },
        },
      },
    ],
  };
  // console.log(`一共有${grammar.BNF.length}个BNF`);//担心有的bnf没有设置action,这里检查一下bnf的数量和action数量是否匹配
  let tscc = new TSCC(grammar, { debug: false, language: 'zh-cn' });
  let compilerSorce = tscc.generate({ genDts: true });
  if (compilerSorce == undefined) {
    throw '构造SQL解析器失败';
  } else {
    fs.writeFileSync('./src/tools/SQLParser.ts', compilerSorce.code);
    fs.writeFileSync('./src/tools/SQLParserDeclare.d.ts', compilerSorce.dts);
    console.log('SQL解析器生成成功');
  }
}
gen();
