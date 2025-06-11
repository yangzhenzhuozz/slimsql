import { SQLSession } from '../main.js';
let session = new SQLSession();
session.registTableView(
  [
    { id: 1, 姓名: '张三', city: '北京', age: 20, 分数: 95, 性别: '男' },
    { id: 2, 姓名: '李四', city: '上海', age: 22, 分数: 88, 性别: '女' },
    { id: 3, 姓名: '王五', city: '北京', age: 21, 分数: 91, 性别: '男' },
    { id: 4, 姓名: 'lucy', city: '深圳', age: 23, 分数: 77, 性别: '女' },
    { id: 5, 姓名: 'tom', city: null, age: 20, 分数: 88, 性别: '男' },
    { id: 6, 姓名: '赵六', city: '广州', age: 24, 分数: 82, 性别: null },
  ],
  '用户'
);
session.registTableView(
  [
    { 订单号: 101, 用户id: 1, 金额: 200, 状态: '已支付' },
    { 订单号: 102, 用户id: 2, 金额: 150, 状态: '待支付' },
    { 订单号: 103, 用户id: 1, 金额: 300, 状态: '已支付' },
    { 订单号: 104, 用户id: 3, 金额: 120, 状态: '已取消' },
    { 订单号: 105, 用户id: 4, 金额: 180, 状态: '已支付' },
    { 订单号: 106, 用户id: 6, 金额: 210, 状态: '已支付' },
  ],
  'B'
);
session.registTableView(
  [
    { 支付id: 1001, 订单号: 101, 支付方式: '支付宝', 支付时间: '2024-06-01' },
    { 支付id: 1002, 订单号: 103, 支付方式: '微信', 支付时间: '2024-06-02' },
    { 支付id: 1003, 订单号: 105, 支付方式: '支付宝', 支付时间: '2024-06-03' },
    { 支付id: 1004, 订单号: 106, 支付方式: '银行卡', 支付时间: '2024-06-04' },
  ],
  'C'
);
console.time('Execution Time');
let ret = session.sql(`
select 'abc' not rlike 'b' from 用户 limit 1
`);
console.table(ret.data);
console.timeEnd('Execution Time');
