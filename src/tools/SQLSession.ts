import { assert } from './assert.js';
import { WindowFunction, Row, ExpNode, SelectClause } from './ExpTree.js';
import { Lexical } from './Lexical.js';
import Parse from './SQLParser.js';
export type FieldType = {
  name: string;
  type: 'string' | 'number' | 'bigint' | 'boolean' | 'symbol' | 'undefined' | 'object' | 'function';
};
export type UDFHanler =
  | {
      type: 'normal';
      handler: (...args: any[]) => any | undefined;
    }
  | {
      type: 'aggregate';
      handler: (list: any[][], isEmpty: boolean, modifier?: 'distinct' | 'all') => any | undefined;
    }
  | {
      type: 'windowFrame';
      handler: (list: any[][], frameInfo: WindowFunction) => any[] | undefined; //传入的frame已经排好序了
    };
export type UDF = {
  [key: string]: UDFHanler;
};

export class SQLSession {
  public tableView: {
    [key: string]: {
      data: Row[];
      fields: Set<string>;
    };
  } = {};
  public udf: UDF = {
    round: {
      type: 'normal',
      handler: (v: number | bigint, dig?: number) => {
        if (v === undefined) {
          throw `round参数不能为空`;
        }
        if (v === null) {
          return null;
        }
        if (typeof v !== 'number') {
          throw `round函数只能处理number`;
        }
        if (dig !== undefined) {
          return v.toFixed(dig);
        } else {
          return Math.round(v);
        }
      },
    },
    concat: {
      type: 'normal',
      handler: (...args) => {
        if (args.length == 0) {
          throw `concat参数不能为空`;
        }
        if (args.length == 1) {
          return `${args[0]}`;
        }
        let ret = '';
        for (let i = 0; i < args.length; i++) {
          if (args[i] === null) {
            return null;
          }
          ret += `${args[i]}`;
        }
        return ret;
      },
    },
    split: {
      type: 'normal',
      handler: (str: string, pattern: string) => {
        if (str === null || pattern === null) {
          return null;
        }
        return str.split(pattern);
      },
    },
    avg: {
      type: 'aggregate',
      handler: (list, isEmpty, modifier?: 'distinct' | 'all') => {
        if (isEmpty) {
          return null;
        }
        if (list[0].length == 0) {
          throw `list函数的参数不能为空`;
        }
        list = list.filter((i) => i[0] !== null);
        if (list.length == 0) {
          return null;
        }
        let count = 0;
        let sum = 0;
        for (let line of list) {
          assert(typeof line[0] == 'number', 'avg只能累加数字');
          if (line[0] !== null) {
            count++;
            sum += line[0];
          }
        }
        return count === 0 ? null : sum / count; //只取第一列的值累加
      },
    },
    max: {
      type: 'aggregate',
      handler: (list, isEmpty, modifier?: 'distinct' | 'all') => {
        if (isEmpty) return null;
        if (list[0].length == 0) throw `max函数的参数不能为空`;
        list = list.filter((i) => i[0] !== null);
        if (list.length == 0) return null;
        assert(typeof list[0][0] == 'number', 'max只能比较数字');
        let values: number[];
        if (modifier === 'all') {
          throw `还不支持modifier:all`;
        } else if (modifier === 'distinct') {
          values = Array.from(new Set(list.map((item) => item[0])));
        } else {
          values = list.map((item) => item[0]);
        }
        let maxVal = -Infinity;
        for (const v of values) {
          if (v > maxVal) maxVal = v;
        }
        return maxVal;
      },
    },
    min: {
      type: 'aggregate',
      handler: (list, isEmpty, modifier?: 'distinct' | 'all') => {
        if (isEmpty) {
          return null;
        }
        if (list[0].length == 0) {
          throw `min函数的参数不能为空`;
        }
        list = list.filter((i) => i[0] !== null);
        if (list.length == 0) {
          return null;
        }
        assert(typeof list[0][0] == 'number', 'min只能比较数字');
        let values: number[];
        if (modifier === 'all') {
          throw `还不支持modifier:all`;
        } else if (modifier === 'distinct') {
          values = Array.from(new Set(list.map((item) => item[0])));
        } else {
          values = list.map((item) => item[0]);
        }
        let minVal = Infinity;
        for (const v of values) {
          if (v < minVal) minVal = v;
        }
        return minVal;
      },
    },
    count: {
      type: 'aggregate',
      handler: (list, isEmpty, modifier?: 'distinct' | 'all') => {
        if (isEmpty) {
          return 0;
        }
        if (list[0].length == 0) {
          throw `count函数的参数不能为空`;
        }
        list = list.filter((i) => i[0] !== null);
        if (list.length == 0) {
          return 0;
        }
        if (modifier === 'all') {
          throw `还不支持modifier:all`;
        } else if (modifier === 'distinct') {
          let set = new Set(list.map((item) => item[0]));
          return set.size;
        } else {
          return list.length;
        }
      },
    },
    sum: {
      type: 'aggregate',
      handler: (list, isEmpty, modifier?: 'distinct' | 'all') => {
        if (isEmpty) {
          return null;
        }
        if (list[0].length == 0) {
          throw `sum函数的参数不能为空`;
        }
        list = list.filter((i) => i[0] !== null);
        if (list.length == 0) {
          return null;
        }
        assert(typeof list[0][0] == 'number', 'sum只能累加数字');

        if (modifier === 'all') {
          throw `还不支持modifier:all`;
        } else if (modifier === 'distinct') {
          let set = new Set(list.map((item) => item[0]));
          return [...set].reduce((p, c) => <number>p + <number>c); //只取第一列的值累加
        } else {
          return list.map((item) => item[0]).reduce((p, c) => <number>p + <number>c); //只取第一列的值累加
        }
      },
    },
    row_number: {
      type: 'windowFrame',
      handler: (list, frameInfo) => {
        if (list[0][0] !== undefined) {
          throw `row_number函数不需要参数`;
        }
        let ret = [] as number[];
        for (let i = 1; i < list.length + 1; i++) {
          ret.push(i);
        }
        return ret;
      },
    },
    rank: {
      type: 'windowFrame',
      handler: (list, frameInfo) => {
        if (list[0][0] !== undefined) {
          throw `rank函数不需要参数`;
        }
        let ret = [1] as number[];
        let rank = 1;
        assert(frameInfo.order !== undefined, 'rank函数必须有排序信息');
        assert(frameInfo.orderedFrame !== undefined, 'rank函数必须有分区信息');
        let compare = (a: any, b: any, keys: string[]) => {
          //a,b都是同一种类型的对象
          for (let k of keys) {
            if (a[k] < b[k]) {
              return -1;
            } else if (a[k] > b[k]) {
              return 1;
            }
          }
          return 0;
        };
        let compareList = frameInfo.order?.map((item) => item.targetName);
        for (let i = 1; i < frameInfo.orderedFrame.length; i++) {
          if (compare(frameInfo.orderedFrame[i - 1], frameInfo.orderedFrame[i], compareList) != 0) {
            rank = i + 1;
          }
          ret.push(rank);
        }
        return ret;
      },
    },
  };
  public registTableView(dataset: Row[], tableName: string, fields?: Set<string>) {
    if (this.tableView[tableName] !== undefined) {
      console.log(`表:${tableName}已经存在,进行替换`);
    }
    this.tableView[tableName] = {
      data: dataset,
      fields: fields ?? new Set(Object.keys(dataset[0])),
    };
  }
  public reisgerUDF(name: string, obj: UDFHanler) {
    this.udf[name] = obj;
  }
  public sql(src: string): { data: Row[]; fields: Set<string> } {
    return Parse(new Lexical(src), {
      session: this,
      ctx: undefined,
      ctxStack: [] as SQLContext[],
    });
  }
}
export class SQLContext {
  public directFieldT: { [key: string]: string } = {}; //可以通过字段名直接访问的属性,value是表名
  private duplicateFieldT: { [key: string]: Set<string> } = {}; //重复的属性,value是表名
  public intermediatView: {
    [key: string]: {
      data: Row[];
      fields: Set<string>;
    };
    [key: symbol]: Row[];
  } = {
    [Symbol.for('@groupDS')]: [],
    [Symbol.for('@windowFrameDS')]: [],
  };
  private rowSize = -1;
  private aggregateWithoutGroupClause = false; //是否在没有使用group子句的时候就用了聚合函数
  private udf: UDF;
  private computedData = [] as Row[]; //用于存放各个表达式计算结果
  private select_normal = false;
  public constructor(udf: UDF) {
    this.udf = udf;
  }
  public addTV(
    view:
      | {
          data: Row[];
          fields: Set<string>;
        }
      | Row[],
    name: string | symbol
  ) {
    if (typeof name === 'symbol') {
      this.intermediatView[name] = view as Row[];
    } else {
      if (this.rowSize === -1) {
        this.rowSize = (
          view as {
            data: Row[];
            fields: Set<string>;
          }
        ).data.length;
        this.computedData = Array.from({ length: this.rowSize }, () => ({}));
      }
      this.intermediatView[name] = view as {
        data: Row[];
        fields: Set<string>;
      };
      let newFields = (
        view as {
          data: Row[];
          fields: Set<string>;
        }
      ).fields;
      for (let f of newFields) {
        if (this.directFieldT[f] !== undefined) {
          let targetTable = this.directFieldT[f];
          delete this.directFieldT[f];
          if (this.duplicateFieldT[f] === undefined) {
            this.duplicateFieldT[f] = new Set();
          }
          this.duplicateFieldT[f].add(targetTable);
          this.duplicateFieldT[f].add(name);
        } else if (this.duplicateFieldT[f] != undefined) {
          this.duplicateFieldT[f].add(name);
        } else {
          this.directFieldT[f] = name;
        }
      }
    }
  }
  /**
   * 深度遍历执行,一旦开始执行，可以保证所有的表行数一致
   * @param exp
   * @param rowIdx
   * @param isRecursive 是否递归调用
   * @param inAggregate 当前表达式是否为聚合函数的参数
   * @returns
   */
  private execExp(exp: ExpNode, rowIdx: number, callerOption: { isRecursive: boolean; inAggregate: boolean }): ExpNode {
    //避免字段名和计算结果一样，比如取一个名字叫做`concat(id,'a')`的字段，会造成误判
    if (exp.op != 'immediate_val' && exp.op != 'alias' && exp.op != 'getfield' && exp.op != 'getTableField' && this.computedData[rowIdx] != undefined && this.computedData[rowIdx][exp.targetName] !== undefined) {
      return {
        op: 'immediate_val',
        targetName: exp.targetName,
        value: this.computedData[rowIdx][exp.targetName],
      };
    }

    let { op, children } = exp;
    let result: any | undefined = undefined;
    let l_Child: ExpNode;
    let r_Child: ExpNode;
    let usedaggregate = false;
    switch (op) {
      case 'immediate_val':
        result = exp.value;
        break;
      case 'alias':
        result = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
        break;
      case 'getTableField':
        {
          let [tableName, fieldName] = (<string>exp.value).split('.');
          let data = this.intermediatView[tableName];
          if (data === undefined) {
            throw `无效表名:${tableName}`;
          }
          if (!data.fields.has(fieldName)) {
            throw `无效字段:${tableName}.${fieldName}`;
          }
          result = data.data[rowIdx][fieldName];
        }
        break;
      case 'getfield':
        {
          let fieldName = <string>exp.value;
          let tableName = this.directFieldT[fieldName];
          if (tableName === undefined) {
            throw `无效字段:${fieldName}`;
          }
          let data = this.intermediatView[tableName];
          result = data.data[rowIdx][fieldName];
        }
        break;
      case 'mod':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (typeof l_Child.value === 'number' && typeof r_Child.value === 'number') {
          result = l_Child.value! % r_Child.value!;
        } else {
          throw 'Unsupported type';
        }
        break;
      case 'add':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          if (typeof l_Child.value !== 'number' || typeof r_Child.value !== 'number') {
            result = l_Child.value!.toString() + r_Child.value!.toString();
          } else {
            result = l_Child.value! + r_Child.value!;
          }
        }
        break;
      case 'sub':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          if (typeof l_Child.value === 'number' && typeof r_Child.value === 'number') {
            result = l_Child.value! - r_Child.value!;
          } else {
            throw 'Unsupported type';
          }
        }
        break;
      case 'mul':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          if (typeof l_Child.value === 'number' && typeof r_Child.value === 'number') {
            result = l_Child.value! * r_Child.value!;
          } else {
            throw 'Unsupported type';
          }
        }
        break;
      case 'div':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          if (typeof l_Child.value === 'number' && typeof r_Child.value === 'number') {
            result = l_Child.value! / r_Child.value!;
          } else {
            throw 'Unsupported type';
          }
        }
        break;
      case 'gt':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          result = l_Child.value! > r_Child.value!;
        }
        break;
      case 'ge':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          result = l_Child.value! >= r_Child.value!;
        }
        break;
      case 'lt':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          result = l_Child.value! < r_Child.value!;
        }
        break;
      case 'le':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          result = l_Child.value! <= r_Child.value!;
        }
        break;
      case 'eq':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          result = l_Child.value! == r_Child.value!;
        }
        break;
      case 'ne':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          result = l_Child.value != r_Child.value;
        }
        break;
      case 'and':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value !== null && !!l_Child.value === false) {
          result = false;
        } else {
          r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
          if ((l_Child.value === null && r_Child.value === null) || (!!l_Child.value && r_Child.value === null) || (l_Child.value === null && !!r_Child.value)) {
            result = null;
          } else {
            result = !!(l_Child.value! && r_Child.value!);
          }
        }
        break;
      case 'or':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (!!l_Child.value) {
          result = true;
        } else {
          r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
          if ((l_Child.value === null && r_Child.value === null) || (l_Child.value !== null && !!l_Child.value == false && r_Child.value === null) || (l_Child.value === null && r_Child.value !== null && !!r_Child.value == false)) {
            result = null;
          } else {
            result = !!(l_Child.value || r_Child.value);
          }
        }
        break;
      case 'index':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          result = l_Child.value[r_Child.value];
        }
        break;
      case 'not':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });

        if (l_Child.value === null) {
          result = null;
        } else {
          result = !l_Child.value!;
        }
        break;
      case 'not-in':
        assert(children != undefined, 'not in子句的children不可能为空');
        l_Child = this.execExp(children[0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null) {
          result = null;
        } else {
          let ret: boolean | null = true; //默认是true,如果有匹配到值,则返回false
          let hasNull = false;
          for (let i = 1; i < children.length; i++) {
            let v = this.execExp(children[i], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
            if (v === null) {
              hasNull = true;
            }
            if (v === l_Child.value) {
              ret = false; //如果有匹配到值,则返回false
              break;
            }
          }
          if (ret === true && hasNull) {
            ret = null; //如果in list中有null,且没有匹配到值,则返回null
          }
          result = ret;
        }
        break;
      case 'in':
        assert(children != undefined, 'in子句的children不可能为空');
        l_Child = this.execExp(children[0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null) {
          result = null;
        } else {
          let ret: boolean | null = false;
          let hasNull = false;
          for (let i = 1; i < children.length; i++) {
            let v = this.execExp(children[i], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
            if (v === null) {
              hasNull = true;
            }
            if (v === l_Child.value) {
              ret = true;
              break;
            }
          }
          if (ret === false && hasNull) {
            ret = null; //如果in list中有null,且没有匹配到值,则返回null
          }
          result = ret;
        }
        break;
      case 'rlike':
        assert(children != undefined, 'rlike子句的children不可能为空');
        l_Child = this.execExp(children[0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children[1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          if (typeof l_Child.value !== 'string' || typeof r_Child.value !== 'string') {
            throw 'rlike的参数必须是string类型';
          }
          result = l_Child.value!.match(new RegExp(r_Child.value!)) !== null;
        }
        break;
      case 'not-rlike':
        assert(children != undefined, 'rlike子句的children不可能为空');
        l_Child = this.execExp(children[0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        r_Child = this.execExp(children[1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null || r_Child.value === null) {
          result = null;
        } else {
          if (typeof l_Child.value !== 'string' || typeof r_Child.value !== 'string') {
            throw 'rlike的参数必须是string类型';
          }
          result = !(l_Child.value!.match(new RegExp(r_Child.value!)) !== null);
        }
        break;
      case 'like':
        {
          assert(children != undefined, 'like子句的children不可能为空');
          l_Child = this.execExp(children[0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
          r_Child = this.execExp(children[1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
          if (l_Child.value === null || r_Child.value === null) {
            result = null;
          } else {
            if (typeof l_Child.value !== 'string' || typeof r_Child.value !== 'string') {
              throw 'like的参数必须是string类型';
            }
            let regexStr = '^';
            let escape = false;
            let input = l_Child.value;
            let pattern = r_Child.value;

            for (const c of pattern) {
              if (escape) {
                // 处理转义字符（如 \%、\_ 或 \\）
                regexStr += c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); //把正则表达式的特殊字符转换成一个标记,$&表示原始字符,即把'('替换成'\('
                escape = false;
              } else if (c === '\\') {
                // 开始转义
                escape = true;
              } else {
                // 处理通配符和普通字符
                switch (c) {
                  case '%':
                    regexStr += '.*';
                    break;
                  case '_':
                    regexStr += '.';
                    break;
                  default:
                    regexStr += c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); //把正则表达式的特殊字符转换成一个标记,$&表示原始字符,即把'('替换成'\('
                }
              }
            }

            // 处理末尾未闭合的转义符（如模式以 \ 结尾）
            if (escape) {
              regexStr += '\\\\';
            }

            regexStr += '$';
            // 创建正则表达式
            const regex = new RegExp(regexStr);
            result = regex.test(input);
          }
        }
        break;
      case 'not-like':
        {
          assert(children != undefined, 'like子句的children不可能为空');
          l_Child = this.execExp(children[0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
          r_Child = this.execExp(children[1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
          if (l_Child.value === null || r_Child.value === null) {
            result = null;
          } else {
            if (typeof l_Child.value !== 'string' || typeof r_Child.value !== 'string') {
              throw 'like的参数必须是string类型';
            }
            let regexStr = '^';
            let escape = false;
            let input = l_Child.value;
            let pattern = r_Child.value;

            for (const c of pattern) {
              if (escape) {
                // 处理转义字符（如 \%、\_ 或 \\）
                regexStr += c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); //把正则表达式的特殊字符转换成一个标记,$&表示原始字符,即把'('替换成'\('
                escape = false;
              } else if (c === '\\') {
                // 开始转义
                escape = true;
              } else {
                // 处理通配符和普通字符
                switch (c) {
                  case '%':
                    regexStr += '.*';
                    break;
                  case '_':
                    regexStr += '.';
                    break;
                  default:
                    regexStr += c.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'); //把正则表达式的特殊字符转换成一个标记,$&表示原始字符,即把'('替换成'\('
                }
              }
            }

            // 处理末尾未闭合的转义符（如模式以 \ 结尾）
            if (escape) {
              regexStr += '\\\\';
            }

            regexStr += '$';
            // 创建正则表达式
            const regex = new RegExp(regexStr);
            result = !regex.test(input);
          }
        }
        break;
      case 'is_null':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        result = l_Child.value === null;
        break;
      case 'is_not_null':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        result = l_Child.value !== null;
        break;
      case 'cast':
        l_Child = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate });
        if (l_Child.value === null) {
          result = null;
        } else {
          assert(exp.cast_type != undefined);
          switch (exp.cast_type) {
            case 'string':
              result = String(l_Child.value);
              break;
            case 'boolean':
              result = Boolean(l_Child.value);
              break;
            case 'number':
              result = Number(l_Child.value);
              break;
            default:
              result = l_Child.value;
              break;
          }
        }
        break;
      case 'case':
        //如果没有else分支,最后一个是undefined
        for (let i = 0; i < children!.length - 1; i++) {
          let when = this.execExp(children![i].children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
          if (when === true) {
            result = this.execExp(children![i].children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
            break;
          }
        }
        if (result === undefined && children![children!.length - 1] !== undefined) {
          result = this.execExp(children![children!.length - 1].children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
        }
        if (result === undefined) {
          result = null;
        }
        break;
      case 'case-exp':
        let case_exp = this.execExp(children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
        //如果没有else分支,最后一个是undefined
        for (let i = 1; i < children!.length - 1; i++) {
          let when = this.execExp(children![i].children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
          if (when == case_exp) {
            result = this.execExp(children![i].children![1], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
            break;
          }
        }
        if (result === undefined && children![children!.length - 1] !== undefined) {
          result = this.execExp(children![children!.length - 1].children![0], rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value;
        }
        if (result === undefined) {
          result = null;
        }
        break;
      case 'call':
        let fun_name = exp.value as string;
        if (this.udf[fun_name] === undefined) {
          throw `未定义函数:${fun_name}`;
        }
        if (this.udf[fun_name].type == 'aggregate') {
          usedaggregate = true;

          let grouSet:
            | {
                [key: string]: {
                  data: Row[];
                  fields: Set<string>;
                };
              }
            | undefined;

          if (this.intermediatView[Symbol.for('@groupDS')][rowIdx] !== undefined) {
            grouSet = this.intermediatView[Symbol.for('@groupDS')][rowIdx];
          }

          //如果还没有group by或者开窗就使用聚合函数
          if (grouSet === undefined) {
            if (this.select_normal) {
              throw `在还没有使用group by的时候，聚合函数和普通列不能混用`;
            }
            this.intermediatView[Symbol.for('@groupDS')] = [this.intermediatView];
            this.rowSize = Math.min(this.rowSize, 1); //如果是空集扩展的数据集，则保留0
            this.aggregateWithoutGroupClause = true;
            this.intermediatView = {};
          }
          let list = [] as Row[][];
          let frameContext = new SQLContext(this.udf);
          for (let tn of Object.getOwnPropertySymbols(this.intermediatView[Symbol.for('@groupDS')][rowIdx])) {
            frameContext.addTV(this.intermediatView[Symbol.for('@groupDS')][rowIdx][tn], tn);
          }
          for (let tn in this.intermediatView[Symbol.for('@groupDS')][rowIdx]) {
            frameContext.addTV(this.intermediatView[Symbol.for('@groupDS')][rowIdx][tn], tn);
          }
          for (let subLine = 0; subLine < frameContext.rowSize; subLine++) {
            let args = [];
            for (let child of children!) {
              let arg = frameContext.execExp(child, subLine, { isRecursive: true, inAggregate: true }).value! as any;
              args.push(arg);
            }
            list.push(args);
          }
          result = this.udf[fun_name].handler(list, this.rowSize === 0, exp.modifier);
        } else if (this.udf[fun_name].type == 'normal') {
          let args: Row[] = [];
          for (let c of children!) {
            args.push(this.execExp(c, rowIdx, { isRecursive: true, inAggregate: callerOption.inAggregate }).value as any);
          }
          if (exp.modifier != undefined) {
            throw '普通函数不支持modifier';
          }
          result = this.udf[fun_name].handler(...args);
        } else {
          throw '不支持的函数类型';
        }
        break;
      default:
        throw `Undefined opcode: ${op}`;
    }
    if (exp.op != 'immediate_val' && exp.op != 'getfield' && exp.op != 'getTableField') {
      this.computedData[rowIdx][exp.targetName] = result;
    }
    if (!usedaggregate && !callerOption.inAggregate) {
      this.select_normal = true;
    }
    return {
      op: 'immediate_val',
      value: result,
      targetName: exp.targetName,
    };
  }
  public alias(table: string, alias: string) {
    if (this.intermediatView[table] === undefined) {
      throw `表"${table}"不存在`;
    }
    if (table == alias) {
      return; //什么都不用做
    }
    this.intermediatView[alias] = this.intermediatView[table];
    delete this.intermediatView[table];
    for (let field in this.directFieldT) {
      if (this.directFieldT[field] == table) {
        this.directFieldT[field] = alias;
      }
    }
    for (let field in this.duplicateFieldT) {
      if (this.duplicateFieldT[field].has(table)) {
        this.duplicateFieldT[field].delete(table);
        this.duplicateFieldT[field].add(alias);
      }
    }
  }
  public limit(exp: ExpNode) {
    let n1 = exp.limit![0];
    let n2 = exp.limit![1];
    if (n2 === undefined) {
      for (let tn in this.intermediatView) {
        this.intermediatView[tn].data = this.intermediatView[tn].data.slice(0, n1);
      }
      for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
        this.intermediatView[tn] = this.intermediatView[tn].slice(0, n1);
      }
      this.intermediatView[Symbol.for('@groupDS')] = this.intermediatView[Symbol.for('@groupDS')].slice(0, n1);
      this.intermediatView[Symbol.for('@windowFrameDS')] = this.intermediatView[Symbol.for('@windowFrameDS')].slice(0, n1);
      this.computedData = this.computedData.slice(0, n1);
      this.rowSize = Math.min(this.rowSize, n1);
    } else {
      for (let tn in this.intermediatView) {
        this.intermediatView[tn].data = this.intermediatView[tn].data.slice(n1, n1 + n2);
      }
      for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
        this.intermediatView[tn] = this.intermediatView[tn].slice(n1, n1 + n2);
      }
      this.intermediatView[Symbol.for('@groupDS')] = this.intermediatView[Symbol.for('@groupDS')].slice(n1, n1 + n2);
      this.intermediatView[Symbol.for('@windowFrameDS')] = this.intermediatView[Symbol.for('@windowFrameDS')].slice(n1, n1 + n2);
      this.computedData = this.computedData.slice(n1, n1 + n2);
      this.rowSize = Math.min(n2, this.rowSize - n1);
    }
  }
  public groupBy(exps: ExpNode[], groupType: 'frame' | 'group') {
    let groupComputed = Array.from({ length: this.rowSize }, () => ({} as Row));
    let groupKeys = new Set<string>();

    let groupExps = {} as Record<string, ExpNode>;

    for (let exp of exps) {
      groupExps[`${exp.targetName}`] = exp;
    }

    for (let i = 0; i < this.rowSize; i++) {
      let groupValues = [] as Row[];
      for (let exp of exps) {
        let cell = this.execExp(exp, i, { isRecursive: false, inAggregate: false });
        //如果表名或者字段名无效,在execExp这里就抛出异常了
        //只在第一行判断group key
        if (i == 0) {
          if (groupKeys.has(cell.targetName)) {
            throw `group重复属性:${cell.targetName}`;
          }
          groupKeys.add(cell.targetName);
        }
        groupComputed[i][cell.targetName] = cell.value!;
        groupValues.push(cell.value! as any);
      }
      groupComputed[i][Symbol.for('idx')] = i;
      groupComputed[i][Symbol.for('@groupValues')] = groupValues.map((item) => item?.toString()).reduce((p, c) => p + ',' + c);
    }

    let groupObj: Partial<Record<any, Row[]>>;
    if (Object.groupBy != undefined) {
      groupObj = Object.groupBy(groupComputed, (item) => item[Symbol.for('@groupValues')]);
    } else {
      //使用自定义的GroupBy
      function myGroupBy<T, K extends PropertyKey>(items: Iterable<T>, callbackfn: (value: T, index: number) => K): Record<K, T[]> {
        const result = {} as Record<K, T[]>;
        let index = 0;

        for (const item of items) {
          const key = callbackfn(item, index++);
          if (!result[key]) {
            result[key] = [];
          }
          result[key].push(item);
        }

        return result;
      }
      groupObj = myGroupBy(groupComputed, (item) => item[Symbol.for('@groupValues')]);
    }

    let groups = Object.keys(groupObj);

    let groupTV = [] as {
      [key: string]: {
        data: Row[];
        fields: Set<string>;
      };
      [key: symbol]: Row[];
    }[];
    this.intermediatView[Symbol.for('@computedData_of_group')] = this.computedData;
    if (groups.length == 0) {
      let tvFrame = {} as any;
      for (let tn in this.intermediatView) {
        let data = [] as Row[];
        let fields = this.intermediatView[tn].fields;
        tvFrame[tn] = {
          data: data,
          fields: fields,
        };
      }
      for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
        let data = [] as any[];
        tvFrame[tn] = data;
      }
      groupTV.push(tvFrame);
    } else {
      for (let i = 0; i < groups.length; i++) {
        let groupLine = groupObj[groups[i]]!;
        let tvFrame = {} as any;
        for (let tn in this.intermediatView) {
          let data = [] as Row[];
          let fields = this.intermediatView[tn].fields;
          for (let line of groupLine) {
            data.push(this.intermediatView[tn].data[line[Symbol.for('idx')]]);
          }
          tvFrame[tn] = {
            data: data,
            fields: fields,
          };
        }
        for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
          let data = [] as any[];
          for (let line of groupLine) {
            data.push(this.intermediatView[tn][line[Symbol.for('idx')]]);
          }
          tvFrame[tn] = data;
        }
        groupTV.push(tvFrame);
      }
    }

    if (groupType == 'group') {
      this.intermediatView[Symbol.for('@groupDS')] = groupTV;
    } else {
      this.intermediatView[Symbol.for('@windowFrameDS')] = groupTV;
    }
    if (groupType == 'group') {
      //保证预留一行给空集做聚合函数
      let keepFields = {} as { [key: string]: string }; //value是表名
      let newTV: {
        [key: string]: {
          data: Row[];
          fields: Set<string>;
        };
        [k: symbol]: Row[];
      } = {};

      //创建TV
      let fields = Object.keys(groupExps);
      for (let colIdx = 0; colIdx < fields.length; colIdx++) {
        let c = fields[colIdx];
        let exp = groupExps[`${c}`] as ExpNode;
        //只有getfield和getTableField往TV中创建字段
        if (exp.op == 'getfield') {
          let tableName = this.directFieldT[exp.value];
          keepFields[exp.value] = this.directFieldT[exp.value];
          if (newTV[tableName] === undefined) {
            newTV[tableName] = {
              data: [],
              fields: new Set(),
            };
          }
          newTV[tableName].fields.add(c);
        } else if (exp.op == 'getTableField') {
          let [tableName, fieldName] = (<string>exp.value).split('.');
          if (this.directFieldT[fieldName] != undefined) {
            keepFields[fieldName] = this.directFieldT[fieldName];
            if (newTV[tableName] === undefined) {
              newTV[tableName] = {
                data: [],
                fields: new Set(),
              };
            }
            newTV[tableName].fields.add(fieldName);
          }
        } else {
          this.computedData[0][c] = null;
        }
      }

      for (let i = 0; i < groups.length; i++) {
        let groupLine = groupObj[groups[i]]!;
        let fields = Object.keys(groupExps);
        for (let colIdx = 0; colIdx < fields.length; colIdx++) {
          let c = fields[colIdx];
          let exp = groupExps[`${c}`] as ExpNode;
          if (exp.op == 'getfield') {
            let tableName = this.directFieldT[exp.value];
            if (colIdx == 0) {
              newTV[tableName].data.push({});
            }
            newTV[tableName].data[newTV[tableName].data.length - 1][c] = groupLine[0][c];
          } else if (exp.op == 'getTableField') {
            let [tableName, fieldName] = (<string>exp.value).split('.');
            if (colIdx == 0) {
              newTV[tableName].data.push({});
            }
            newTV[tableName].data[newTV[tableName].data.length - 1][fieldName] = groupLine[0][c];
          } else {
            this.computedData[i][c] = groupLine[0][c];
          }
        }
      }
      //复制临时表
      for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
        newTV[tn] = this.intermediatView[tn];
      }
      this.intermediatView = newTV;
      this.directFieldT = keepFields;
      this.rowSize = groups.length;
    }
  }
  /**
   * @param exps
   * @returns
   */
  private orderBy(exps: ExpNode[]) {
    let newOrder = [] as number[];
    for (let i = 0; i < this.rowSize; i++) {
      newOrder.push(i);
    }
    let orderKeys = [] as { name: string; order: 'asc' | 'desc' }[];
    let windowFrameFunctions = [] as WindowFunction[];
    for (let row_idx = 0; row_idx < this.rowSize; row_idx++) {
      for (let exp_idx = 0; exp_idx < exps.length; exp_idx++) {
        let exp = exps[exp_idx];
        let orderKey = exp.targetName;
        //只在第一行判断order Key
        if (row_idx == 0) {
          orderKeys.push({
            name: orderKey,
            order: exp.order!,
          });
        }

        if (exp.children![0].windowFunction !== undefined) {
          if (row_idx == 0) {
            windowFrameFunctions.push(exp.children![0].windowFunction);
          }
        } else {
          let ret = this.execExp(exp.children![0], row_idx, { isRecursive: false, inAggregate: false });
          this.computedData[row_idx][orderKey] = ret.value;
        }
      }
    }

    if (windowFrameFunctions.length > 0) {
      for (let windowFrameFunction of windowFrameFunctions) {
        this.windowFunction(windowFrameFunction);
      }
    }

    let compare = (a: number, b: number): number => {
      for (let k of orderKeys) {
        let va;
        let vb;
        if (this.intermediatView[Symbol.for('frameResult')] !== undefined && this.intermediatView[Symbol.for('frameResult')][a][k.name] !== undefined) {
          va = this.intermediatView[Symbol.for('frameResult')][a][k.name];
        } else {
          va = this.computedData[a][k.name];
        }

        if (this.intermediatView[Symbol.for('frameResult')] !== undefined && this.intermediatView[Symbol.for('frameResult')][b][k.name] !== undefined) {
          vb = this.intermediatView[Symbol.for('frameResult')][b][k.name];
        } else {
          vb = this.computedData[b][k.name];
        }

        if (k.order == 'asc') {
          if (va === null && vb === null) return 0;
          if (va === null) return -1;
          if (vb === null) return 1;

          if (va < vb) {
            return -1;
          } else if (va > vb) {
            return 1;
          }
        } else {
          if (va === null && vb === null) return 0;
          if (vb === null) return -1;
          if (va === null) return 1;

          if (va > vb) {
            return -1;
          } else if (va < vb) {
            return 1;
          }
        }
      }
      return 0;
    };

    newOrder.sort(compare);

    let reOrderData = (data: any[], idxList: number[]): any[] => {
      if (idxList.length == 0) {
        return data;
      }
      let ret = [];
      for (let idx of idxList) {
        ret.push(data[idx]);
      }
      return ret;
    };
    for (let tn in this.intermediatView) {
      this.intermediatView[tn].data = reOrderData(this.intermediatView[tn].data, newOrder);
    }
    for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
      this.intermediatView[tn] = reOrderData(this.intermediatView[tn], newOrder);
    }
    this.computedData = reOrderData(this.computedData, newOrder);
    this.select_normal = false; //因为使用到了execExp，所以重置
  }
  public select(select_clause: SelectClause, orderClause?: ExpNode[]): { data: Row[]; fields: Set<string> } {
    let exps = select_clause.nodes;
    let arr = [] as any[];
    let windowFrames = [] as WindowFunction[];
    let directField = new Set<string>(); //直接选择的ID
    let duplicateField = new Set<string>(); //select中有重复的ID
    for (let exp of exps) {
      let id: string | undefined;
      if (exp.op === 'getTableField') {
        id = exp.targetName.split('.')[1];
      } else if (exp.op === 'getfield') {
        id = exp.targetName;
      } else if (exp.op === 'windowFunction') {
        windowFrames.push(exp.windowFunction!);
        if (exp.windowFunction?.alias !== undefined) {
          id = exp.windowFunction?.alias;
        }
      }
      if (id !== undefined) {
        if (directField.has(id)) {
          duplicateField.add(id);
        } else {
          directField.add(id);
        }
      }
    }
    let retFields = new Set<string>();
    for (let exp of exps) {
      let field: string | undefined;
      if (exp.op === 'getTableField') {
        field = exp.targetName.split('.')[1];
        if (!duplicateField.has(field)) {
          retFields.add(field);
        } else {
          retFields.add(exp.targetName);
        }
      } else if (exp.op === 'windowFunction') {
        if (exp.windowFunction?.alias !== undefined) {
          field = exp.windowFunction?.alias;
          if (!duplicateField.has(field)) {
            retFields.add(field);
          } else {
            retFields.add(exp.targetName);
          }
        }
      } else {
        retFields.add(exp.targetName);
      }
    }

    if (windowFrames.length > 0) {
      for (let windowFrame of windowFrames) {
        this.windowFunction(windowFrame);
      }
    }

    //如果有distinct，则等group by之后再order by
    if (select_clause.modifier !== 'distinct') {
      if (orderClause !== undefined) {
        this.orderBy(orderClause);
      }
    }

    //如果是一个空集，需要特殊处理
    if (this.rowSize === 0) {
      for (let tn in this.intermediatView) {
        let fields = this.intermediatView[tn].fields;
        let nullRow = {} as Row;
        for (let k of fields) {
          nullRow[k] = null;
        }
        this.intermediatView[tn].data = [nullRow];
      }
      for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
        this.intermediatView[tn] = [{}];
      }
      this.intermediatView[Symbol.for('frameResult')] = [{}];
      this.computedData = Array.from({ length: 1 }, () => ({}));
    }
    //保证至少有一行
    for (let row_idx = 0; row_idx < Math.max(this.rowSize, 1); row_idx++) {
      let tmpRow = {} as any;
      //处理每一列
      for (let exp_idx = 0; exp_idx < exps.length; exp_idx++) {
        let exp = exps[exp_idx];
        if (exp.op === 'windowFunction') {
          let k = exp.windowFunction!.alias ?? exp.targetName;
          tmpRow[k] = this.intermediatView[Symbol.for('frameResult')][row_idx][exp.targetName]; //占位
        } else {
          let cell = this.execExp(exp, row_idx, { isRecursive: false, inAggregate: false });
          let fieldName: string;
          if (exp.op === 'getTableField') {
            let id = exp.targetName.split('.')[1];
            if (!duplicateField.has(id)) {
              fieldName = id;
            } else {
              fieldName = cell.targetName;
            }
          } else {
            fieldName = cell.targetName;
          }
          if (tmpRow[fieldName] !== undefined) {
            throw `select重复属性:${cell.targetName!}`;
          }
          tmpRow[fieldName] = cell.value!;
        }
      }
      arr.push(tmpRow);
    }
    //如果本来就是空集，而且没有集合函数对其进行处理，则继续返回空集
    if (this.rowSize == 0 && !this.aggregateWithoutGroupClause) {
      arr = [];
    }

    let ret = {
      data: arr,
      fields: retFields,
    } as { data: Row[]; fields: Set<string> };

    if (select_clause.modifier === 'all') {
      throw `不支持all`;
    } else if (select_clause.modifier === 'distinct') {
      let distinct_nodes = [] as ExpNode[];
      for (let k in arr[0]) {
        distinct_nodes.push({
          op: 'getfield',
          value: k,
          targetName: k,
        });
      }
      //这里的递归调用保证select不会再使用modifier，所以不会变成无线递归
      let groupCtx = new SQLContext(this.udf);
      groupCtx.addTV(ret, 'distinct_result');
      groupCtx.groupBy(distinct_nodes, 'group');
      ret = groupCtx.select({ nodes: distinct_nodes }, orderClause); //使用group by去重
    }

    //清理tv、direct、duplicate
    this.intermediatView = {};
    this.directFieldT = {};
    this.computedData = [];
    this.select_normal = false;
    this.rowSize = -1;

    return ret;
  }
  private windowFunction(fn: WindowFunction) {
    if (this.rowSize == 0) {
      return;
    }
    //对每一个窗口帧进行处理
    let field_key = fn.targetName;
    if (this.intermediatView[Symbol.for('frameResult')] !== undefined && this.intermediatView[Symbol.for('frameResult')].length > 0 && this.intermediatView[Symbol.for('frameResult')][0][field_key] !== undefined) {
      //如果已经计算过了,则直接返回
      return;
    }
    this.groupBy(fn.partition, 'frame'); //各个不同分区的frame
    const windowFrameDS = this.intermediatView[Symbol.for('@windowFrameDS')];
    this.intermediatView = {}; //清除tv，因为开窗会改变顺序
    for (let frame of windowFrameDS) {
      let frameContext = new SQLContext(this.udf);
      for (let tn in frame) {
        frameContext.addTV(frame[tn], tn);
      }

      for (let tn of Object.getOwnPropertySymbols(frame)) {
        frameContext.addTV(frame[tn], tn);
      }

      let orderedFrame = [] as Row[];
      if (fn.order !== undefined) {
        frameContext.orderBy(fn.order);
        orderedFrame = frameContext.computedData;
      }

      if (frameContext.intermediatView[Symbol.for('frameResult')] === undefined) {
        frameContext.intermediatView[Symbol.for('frameResult')] = Array.from({ length: frameContext.rowSize }, () => ({}));
      }
      if (this.udf[fn.windowFunction.value! as string].type == 'aggregate') {
        //聚合函数使用窗口范围

        //如果窗口范围是unbounded,则直接使用聚合函数的值填充
        //比如sum() over (partition by id order by name rows between unbounded preceding and unbounded following)
        if (fn.frameRange.start.offset == 'unbounded' && fn.frameRange.start.type == 'preceding' && fn.frameRange.end.offset == 'unbounded' && fn.frameRange.end.type == 'following') {
          frameContext.intermediatView[Symbol.for('@groupDS')] = [frameContext.intermediatView];
          let aggregateVal = frameContext.execExp(fn.windowFunction, 0, { isRecursive: false, inAggregate: false }).value as Row;
          for (let i = 0; i < frameContext.rowSize; i++) {
            frameContext.intermediatView[Symbol.for('frameResult')][i][field_key] = aggregateVal;
          }
        } else {
          for (let frameRowidx = 0; frameRowidx < frameContext.rowSize; frameRowidx++) {
            let start = 0;
            let end = 0;
            let getRowIndex = (offset: number | 'unbounded', type: 'preceding' | 'following' | 'current row') => {
              if (type == 'preceding') {
                if (offset == 'unbounded') {
                  return 0;
                } else {
                  return frameRowidx - offset;
                }
              } else if (type == 'following') {
                if (offset == 'unbounded') {
                  return frameContext.rowSize - 1;
                } else {
                  return frameRowidx + offset;
                }
              } else if (type == 'current row') {
                return frameRowidx;
              } else {
                throw `不支持的窗口范围类型:${type}`;
              }
            };
            start = Math.max(getRowIndex(fn.frameRange.start.offset, fn.frameRange.start.type), 0);
            end = Math.min(getRowIndex(fn.frameRange.end.offset, fn.frameRange.end.type), frameContext.rowSize - 1) + 1; //end是闭区间，所以+1
            if (start > end) {
              throw `窗口范围错误,开始行:${start}大于结束行:${end}`;
            }
            let tmpTV: {
              [key: string]: {
                data: Row[];
                fields: Set<string>;
              };
            } = {};
            let originTV = frameContext.intermediatView;
            for (let tn in frameContext.intermediatView) {
              tmpTV[tn] = {
                data: frameContext.intermediatView[tn].data.slice(start, end),
                fields: frameContext.intermediatView[tn].fields,
              };
            }
            frameContext.intermediatView = tmpTV;
            frameContext.intermediatView[Symbol.for('@groupDS')] = [tmpTV];
            let aggregateVal = frameContext.execExp(fn.windowFunction, 0, { isRecursive: false, inAggregate: false }).value as Row;
            frameContext.intermediatView = originTV;
            frameContext.intermediatView[Symbol.for('frameResult')][frameRowidx][field_key] = aggregateVal;
          }
        }
        for (let tn in frameContext.intermediatView) {
          if (this.intermediatView[tn] === undefined) {
            this.intermediatView[tn] = {
              data: frameContext.intermediatView[tn].data,
              fields: frameContext.intermediatView[tn].fields,
            };
          } else {
            this.intermediatView[tn].data = this.intermediatView[tn].data.concat(frameContext.intermediatView[tn].data);
          }
        }
        for (let tn of Object.getOwnPropertySymbols(frameContext.intermediatView)) {
          if (this.intermediatView[tn] === undefined) {
            this.intermediatView[tn] = [];
          }
          this.intermediatView[tn] = this.intermediatView[tn].concat(frameContext.intermediatView[tn]);
        }
      } else if (this.udf[fn.windowFunction.value! as string].type == 'windowFrame') {
        //窗口函数不使用窗口范围
        if (fn.windowFunction.modifier != undefined) {
          throw '开窗函数不支持modifier';
        }
        let list = [] as any[][];
        for (let row_idx = 0; row_idx < frameContext.rowSize; row_idx++) {
          let args = [];
          for (let child of fn.windowFunction.children!) {
            let arg = frameContext.execExp(child, row_idx, { isRecursive: false, inAggregate: false }).value! as any;
            args.push(arg);
          }
          list.push(args);
        }
        fn.orderedFrame = orderedFrame;
        let windowFrameVals = this.udf[fn.windowFunction.value].handler(list, fn) as any[];
        for (let i = 0; i < windowFrameVals.length; i++) {
          frameContext.intermediatView[Symbol.for('frameResult')][i][field_key] = windowFrameVals[i];
        }
        for (let tn in frameContext.intermediatView) {
          if (this.intermediatView[tn] === undefined) {
            this.intermediatView[tn] = {
              data: frameContext.intermediatView[tn].data,
              fields: frameContext.intermediatView[tn].fields,
            };
          } else {
            this.intermediatView[tn].data = this.intermediatView[tn].data.concat(frameContext.intermediatView[tn].data);
          }
        }
        for (let tn of Object.getOwnPropertySymbols(frameContext.intermediatView)) {
          if (this.intermediatView[tn] === undefined) {
            this.intermediatView[tn] = [];
          }
          this.intermediatView[tn] = this.intermediatView[tn].concat(frameContext.intermediatView[tn]);
        }
      } else {
        throw `不支持的窗口函数类型:${this.udf[fn.windowFunction.value! as string].type}`;
      }
    }
    this.computedData = []; //重置缓存
    for (let line of this.intermediatView[Symbol.for('@computedData_of_group')]) {
      this.computedData.push(line);
    }
    delete this.intermediatView[Symbol.for('@computedData_of_group')];
  }
  private leftCross(idxs1: number[], idxs2: number[]): [number, number][] {
    let ret = [] as [number, number][];
    for (let idx1 of idxs1) {
      for (let idx2 of idxs2) {
        ret.push([idx1, idx2]);
      }
    }
    return ret;
  }
  private sortMergeLeftJoin(option: { t1: string; id1: string; t2: string; id2: string }): [number, number | null][] {
    let ret = [] as [number, number | null][];
    let values1 = [] as { v: any; idx: number }[];
    let values2 = [] as { v: any; idx: number }[];
    for (let row_idx = 0; row_idx < this.rowSize; row_idx++) {
      let v = this.execExp(
        {
          op: 'getTableField',
          value: `${option.t1}.${option.id1}`,
          targetName: `${option.t1}.${option.id1}`,
        },
        row_idx,
        { isRecursive: false, inAggregate: false }
      ).value;
      values1.push({ v: v, idx: row_idx });
    }
    for (let row_idx = 0; row_idx < this.intermediatView[option.t2].data.length; row_idx++) {
      let v = this.execExp(
        {
          op: 'getTableField',
          value: `${option.t2}.${option.id2}`,
          targetName: `${option.t2}.${option.id2}`,
        },
        row_idx,
        { isRecursive: false, inAggregate: false }
      ).value;
      values2.push({ v: v, idx: row_idx });
    }

    //比较器
    let comparator = (a: { v: any; idx: number }, b: { v: any; idx: number }): number => {
      //让null小于非null，如果两个都是null，在join之前会被特殊判断
      if (a.v === null && b.v === null) return 0;
      if (a.v === null) return -1;
      if (b.v === null) return 1;

      if (a.v < b.v) {
        return -1;
      } else if (a.v > b.v) {
        return 1;
      } else {
        return 0;
      }
    };
    //搜索窗口区间
    let windowFrame = (arr: { v: any; idx: number }[], start: number) => {
      if (start >= arr.length) {
        return 0;
      }
      let v = arr[start].v;
      let idx = start + 1;
      for (; idx < arr.length && arr[idx].v == v; idx++) {}
      return idx - start;
    };

    //先排序
    values1.sort(comparator);
    values2.sort(comparator);

    //开始进行连接
    let idx1 = 0;
    let idx2 = 0;
    for (; idx1 < values1.length; ) {
      let cmp: number;
      if (idx2 < values2.length) {
        //两边都是null就让右表先走
        if (values1[idx1].v === null && values2[idx2].v === null) {
          cmp = 1;
        } else {
          cmp = comparator(values1[idx1], values2[idx2]);
        }
      } else {
        cmp = 1; //arr2已经走完了
      }
      let w1 = windowFrame(values1, idx1);
      let w2 = windowFrame(values2, idx2);
      let frame1 = values1.slice(idx1, idx1 + w1).map((item) => item.idx);
      if (cmp < 0 || w2 == 0) {
        for (let fr of frame1) {
          ret.push([fr, null]);
        }
        idx1 += w1;
      } else if (cmp > 0) {
        idx2 += w2;
      } else {
        let frame2 = values2.slice(idx2, idx2 + w2).map((item) => item.idx);
        ret = ret.concat(this.leftCross(frame1, frame2));
        idx1 += w1;
        idx2 += w2;
      }
    }

    return ret;
  }
  public leftJoin(lefts: string[], right: string, exp: ExpNode): string[] {
    let joinResult: [number, number | null][];
    let isFastJoin = false;
    if (this.rowSize == 0) {
      joinResult = [];
    } else if (this.intermediatView[right].data.length == 0) {
      joinResult = [];
      for (let i = 0; i < this.rowSize; i++) {
        joinResult.push([i, null]);
      }
    } else {
      //无需清理tv、direct、duplicate
      if (exp.op == 'eq') {
        let lc = exp.children![0];
        let rc = exp.children![1];
        let t1: string | undefined;
        let t2: string | undefined;
        let id1: string | undefined;
        let id2: string | undefined;
        if (lc.op == 'getfield') {
          t1 = this.directFieldT[lc.value!];
          id1 = lc.value!;
        } else if (lc.op == 'getTableField') {
          [t1, id1] = lc.value!.split('.');
        }
        if (rc.op == 'getfield') {
          t2 = this.directFieldT[rc.value!];
          id2 = rc.value!;
        } else if (rc.op == 'getTableField') {
          [t2, id2] = rc.value!.split('.');
        }
        if (t1 != undefined && t2 != undefined && t1 != t2) {
          //使用加速连接,保证传递给sortMergeLeftJoin的左表放到t1,右表放到t2
          if (right === t2) {
            joinResult = this.sortMergeLeftJoin({ t1: t1!, id1: id1!, t2: t2!, id2: id2! });
          } else {
            joinResult = this.sortMergeLeftJoin({ t1: t2!, id1: id2!, t2: t1!, id2: id1! });
          }

          isFastJoin = true;
        } else {
          console.log('只有当join条件为左右表各取一个字段，并进行等值连接时才有加速效果');
          //使用笛卡尔积
          let idxs1 = [] as number[];
          let idxs2 = [] as number[];
          for (let i = 0; i < this.rowSize; i++) {
            idxs1.push(i);
          }
          for (let i = 0; i < this.intermediatView[right].data.length; i++) {
            idxs2.push(i);
          }
          joinResult = this.leftCross(idxs1, idxs2);
        }
      } else {
        //使用笛卡尔积
        let idxs1 = [] as number[];
        let idxs2 = [] as number[];
        for (let i = 0; i < this.rowSize; i++) {
          idxs1.push(i);
        }
        for (let i = 0; i < this.intermediatView[right].data.length; i++) {
          idxs2.push(i);
        }
        joinResult = this.leftCross(idxs1, idxs2);
      }
    }
    for (let leftT of lefts) {
      let originData = this.intermediatView[leftT].data;
      this.intermediatView[leftT].data = [];
      for (let rowIdx of joinResult) {
        this.intermediatView[leftT].data.push(originData[rowIdx[0]]);
      }
    }
    let originData = this.intermediatView[right].data;
    this.intermediatView[right].data = [];

    this.rowSize = joinResult.length; //修改rowSize
    this.computedData = Array.from({ length: this.rowSize }, () => ({})); //join之后之前的缓存全部失效，可以存放连接条件

    for (let i = 0; i < joinResult.length; i++) {
      let rowIdx = joinResult[i];
      if (rowIdx[1] === null) {
        let nullRow = {} as Row;
        for (let k of this.intermediatView[right].fields) {
          nullRow[k] = null;
        }
        this.intermediatView[right].data.push(nullRow);
        this.computedData[i] = { leftTableIdx: rowIdx[0], joinConditon: true }; //右表没有能匹配的，设置为null,左连接时需要选中
      } else {
        this.intermediatView[right].data.push(originData[rowIdx[1]]);
        let lastRowIdx = this.intermediatView[right].data.length - 1;
        this.computedData[i] = { leftTableIdx: rowIdx[0], joinConditon: this.execExp(exp, lastRowIdx, { isRecursive: false, inAggregate: false }).value };
      }
    }

    //使用笛卡尔积进行连接
    if (!isFastJoin && joinResult.length > 0) {
      let resultIdx = [] as number[];
      let lastAddLeftIdx = -1; //上一次添加的左表下标
      for (let row_idx = 0; row_idx < joinResult.length; row_idx++) {
        if (row_idx > 0) {
          //左表下标已经变化
          if (this.computedData[row_idx].leftTableIdx != this.computedData[row_idx - 1].leftTableIdx) {
            //但是前一行还没有被添加进去,这时候应该手工插入一个空行
            if (lastAddLeftIdx != this.computedData[row_idx - 1].leftTableIdx) {
              let nullRow = {} as Row;
              for (let k of this.intermediatView[right].fields) {
                nullRow[k] = null;
              }
              this.intermediatView[right].data[row_idx - 1] = nullRow;
              resultIdx.push(row_idx - 1);
            }
          }
        }
        let condition = this.computedData[row_idx].joinConditon;
        if (condition) {
          resultIdx.push(row_idx);
          lastAddLeftIdx = this.computedData[row_idx].leftTableIdx;
        }
      }
      if (this.computedData[joinResult.length - 1].leftTableIdx != lastAddLeftIdx) {
        //但是前一行还没有被添加进去,这时候应该手工插入一个空行
        let nullRow = {} as Row;
        for (let k of this.intermediatView[right].fields) {
          nullRow[k] = null;
        }
        this.intermediatView[right].data[joinResult.length - 1] = nullRow;
        resultIdx.push(joinResult.length - 1);
      }

      for (let tn in this.intermediatView) {
        let originData = this.intermediatView[tn].data;
        this.intermediatView[tn].data = [];
        for (let idx of resultIdx) {
          this.intermediatView[tn].data.push(originData[idx]);
        }
      }
      for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
        let originData = this.intermediatView[tn];
        this.intermediatView[tn] = [];
        for (let idx of resultIdx) {
          this.intermediatView[tn].push(originData[idx]);
        }
      }
      {
        let originData = this.intermediatView[Symbol.for('@groupDS')];
        this.intermediatView[Symbol.for('@groupDS')] = [];
        for (let idx of resultIdx) {
          this.intermediatView[Symbol.for('@groupDS')].push(originData[idx]);
        }
      }
      {
        let originData = this.intermediatView[Symbol.for('@windowFrameDS')];
        this.intermediatView[Symbol.for('@windowFrameDS')] = [];
        for (let idx of resultIdx) {
          this.intermediatView[Symbol.for('@windowFrameDS')].push(originData[idx]);
        }
      }
      this.rowSize = resultIdx.length; //修改rowSize
      this.computedData = Array.from({ length: this.rowSize }, () => ({}));
    }

    this.select_normal = false; //因为使用到了execExp，所以重置
    return lefts.concat(right);
  }
  public where(exp: ExpNode) {
    let resultIdx = [] as number[];
    for (let row_idx = 0; row_idx < this.rowSize; row_idx++) {
      let condition = this.execExp(exp, row_idx, { isRecursive: false, inAggregate: false });
      if (condition.value) {
        resultIdx.push(row_idx);
      }
    }
    for (let tn in this.intermediatView) {
      let originData = this.intermediatView[tn].data;
      this.intermediatView[tn].data = [];
      for (let idx of resultIdx) {
        this.intermediatView[tn].data.push(originData[idx]);
      }
    }
    for (let tn of Object.getOwnPropertySymbols(this.intermediatView)) {
      let originData = this.intermediatView[tn];
      this.intermediatView[tn] = [];
      for (let idx of resultIdx) {
        this.intermediatView[tn].push(originData[idx]);
      }
    }
    {
      let originData = this.intermediatView[Symbol.for('@groupDS')];
      this.intermediatView[Symbol.for('@groupDS')] = [];
      for (let idx of resultIdx) {
        this.intermediatView[Symbol.for('@groupDS')].push(originData[idx]);
      }
    }
    {
      let originData = this.intermediatView[Symbol.for('@windowFrameDS')];
      this.intermediatView[Symbol.for('@windowFrameDS')] = [];
      for (let idx of resultIdx) {
        this.intermediatView[Symbol.for('@windowFrameDS')].push(originData[idx]);
      }
    }
    this.select_normal = false; //因为使用到了execExp，所以重置
    this.rowSize = resultIdx.length; //修改rowSize
    this.computedData = Array.from({ length: this.rowSize }, () => ({}));
  }
}
