import { SQLSession } from '../main.js';

let arr = [
  { id: 1, gender: '男', name: 'john', score: 10 },
  { id: 2, gender: '女', name: 'kelly', score: 11 },
  { id: 12, gender: '女', name: 'danny', score: 11 },
  { id: 12, gender: null, name: 'danny', score: 16 },
];
let arr2 = [
  { id: 2, idx: 2, score2: 15 },
  { id: 2, idx: 2, score2: 99 },
  { id: 3, idx: 3, score2: 17 },
];
let arr3 = [
  { id: 2, idx: 2, score2: 15 },
  { id: 2, idx: 2, score2: 99 },
  { id: 3, idx: 3, score2: 17 },
];

//把集合注册到Session中
let session = new SQLSession();
session.registTableView(arr, 't1');
session.registTableView(arr2, 't2');
session.registTableView(arr3, 't3');

console.time('Execution Time');
let ret = session.sql(`
select distinct * from t3
`);
console.table(ret.data);
console.timeEnd('Execution Time');

/*
1、测试alias                                    OK
2、测试order by                                 OK
    1、order by windowFunction                 OK
3、limit a,b    (a+b>rowSize)                   OK
4、测试group by以及group by之后的开窗           OK
    1、group by字段能否select                   OK
    2、group by表达式能否select                 OK
    3、没有使用group by就聚合                   OK 
5、实现windowFunction,并测试在order by中的效果   OK
    1、测试聚合函数全选                          OK
    2、测试聚合函数部分                          OK
    2、测试order by                             OK
6、连接                                        OK
    1、两表连接                                 OK
    2、三表连接                                 OK
    3、连接时左表或者右表没有任何行              OK
7、测试where                                    OK
8、测试                                         OK
    select
      A.id //这里应该报错
    from
      (select * from t1) as A

 */
