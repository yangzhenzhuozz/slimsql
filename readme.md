# 安装
npm i slimsql
# demo

```js
import { SQLSession } from 'slimsql';

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
select
  rank() over(partition by t1.id order by score) as rn , *
from
  t1 left join t2 on t1.id=t2.id
  left join t3 on t2.id=t3.id
`);
console.table(ret.data);
console.timeEnd('Execution Time');

```