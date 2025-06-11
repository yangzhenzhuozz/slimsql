export interface ExpNode {
  op:
    | 'index'
    | 'rlike'
    | 'like'
    | 'not-rlike'
    | 'not-like'
    | 'not-in'
    | 'in'
    | 'limit'
    | 'cast'
    | '*'
    | 'group'
    | 'is_null'
    | 'is_not_null'
    | 'order'
    | 'group_having'
    | 'call'
    | 'getfield'
    | 'getTableField'
    | 'alias'
    | 'mod'
    | 'case'
    | 'case-exp'
    | 'when'
    | 'else'
    | 'add'
    | 'sub'
    | 'mul'
    | 'div'
    | 'lt'
    | 'le'
    | 'eq'
    | 'ne'
    | 'gt'
    | 'ge'
    | 'immediate_val'
    | 'and'
    | 'or'
    | 'not'
    | 'windowFunction';
  targetName: string;
  windowFunction?: WindowFunction;
  children?: ExpNode[];
  value?: any;
  cast_type?: 'string' | 'number' | 'boolean';
  order?: 'asc' | 'desc';
  modifier?: 'distinct' | 'all';
  limit?: number[];
}
export interface SelectClause {
  modifier?: 'distinct' | 'all';
  nodes: ExpNode[];
}
export interface FrameOffset {
  offset: number | 'unbounded';
  type: 'preceding' | 'following' | 'current row';
}
export interface FrameRange {
  start: FrameOffset;
  end: FrameOffset;
}
export interface WindowFunction {
  windowFunction: ExpNode;
  partition: ExpNode[];
  order?: ExpNode[];
  orderedFrame?: any[];
  alias?: string;
  targetName: string;
  frameRange: FrameRange;
}
export interface Row {
  [key: string | number | symbol]: any; // 键可以是字符串、数字或符号
}
