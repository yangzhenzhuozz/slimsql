import test, { describe, beforeEach } from 'node:test';
import assert from 'assert';
import { SQLSession } from '../main.js';

describe('sql解析器功能测试（中文与英文混合数据，小写关键字）', () => {
  let session: SQLSession;

  beforeEach(() => {
    session = new SQLSession();
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
      '订单'
    );

    session.registTableView(
      [
        { 支付id: 1001, 订单号: 101, 支付方式: '支付宝', 支付时间: '2024-06-01' },
        { 支付id: 1002, 订单号: 103, 支付方式: '微信', 支付时间: '2024-06-02' },
        { 支付id: 1003, 订单号: 105, 支付方式: '支付宝', 支付时间: '2024-06-03' },
        { 支付id: 1004, 订单号: 106, 支付方式: '银行卡', 支付时间: '2024-06-04' },
      ],
      '支付'
    );
  });

  test('select * from 用户', () => {
    const ret = session.sql('select * from 用户');
    assert.strictEqual(ret.data.length, 6);
  });

  test("select 姓名, 分数 from 用户 where city = '北京'", () => {
    const ret = session.sql("select 姓名, 分数 from 用户 where city = '北京'");
    assert.deepStrictEqual(ret.data, [
      { 姓名: '张三', 分数: 95 },
      { 姓名: '王五', 分数: 91 },
    ]);
  });

  test('select * from 用户 order by 分数 desc', () => {
    const ret = session.sql('select * from 用户 order by 分数 desc');
    assert.strictEqual(ret.data[0].分数, 95);
  });

  test('select * from 用户 limit 3', () => {
    const ret = session.sql('select * from 用户 limit 3');
    assert.strictEqual(ret.data.length, 3);
  });

  test('select distinct city from 用户', () => {
    const ret = session.sql('select distinct city from 用户');
    assert.strictEqual(ret.data.length, 5);
  });

  test('select 姓名, 分数 + 5 as 新分数 from 用户', () => {
    const ret = session.sql('select 姓名, 分数 + 5 as 新分数 from 用户');
    assert.ok('新分数' in ret.data[0]);
  });

  test('select * from 用户 where city is null', () => {
    const ret = session.sql('select * from 用户 where city is null');
    assert.strictEqual(ret.data.length, 1);
    assert.strictEqual(ret.data[0].姓名, 'tom');
  });

  test('select * from 用户 where city is not null', () => {
    const ret = session.sql('select * from 用户 where city is not null');
    assert.strictEqual(ret.data.length, 5);
  });

  test("select * from 用户 where 姓名 like '%三'", () => {
    const ret = session.sql("select * from 用户 where 姓名 like '%三'");
    assert.strictEqual(ret.data.length, 1);
    assert.strictEqual(ret.data[0].姓名, '张三');
  });

  test('select 用户.姓名, 订单.金额, 支付.支付方式 from 用户 left join 订单 on 用户.id = 订单.用户id left join 支付 on 订单.订单号 = 支付.订单号', () => {
    const ret = session.sql('select 用户.姓名, 订单.金额, 支付.支付方式 from 用户 left join 订单 on 用户.id = 订单.用户id left join 支付 on 订单.订单号 = 支付.订单号');
    assert.ok(ret.data.some((r) => r.支付方式 === '支付宝'));
  });

  test('select 姓名 as name, 分数 as score from 用户 where id = 4', () => {
    const ret = session.sql('select 姓名 as name, 分数 as score from 用户 where id = 4');
    assert.ok('name' in ret.data[0]);
    assert.ok('score' in ret.data[0]);
  });

  test('select * from 用户 where id not in (2, 4)', () => {
    const ret = session.sql('select * from 用户 where id not in (2, 4)');
    assert.strictEqual(ret.data.length, 4);
  });

  test('select 性别, count(1) as 人数 from 用户 group by 性别 having  count(1) > 1', () => {
    const ret = session.sql('select 性别, count(1) as 人数 from 用户 group by 性别 having  count(1) > 1');
    assert.strictEqual(ret.data.length, 2);
  });

  test('select 姓名, 分数, rank() over(order by 分数 desc) as 排名 from 用户', () => {
    const ret = session.sql('select 姓名, 分数, rank() over(order by 分数 desc) as 排名 from 用户');
    assert.strictEqual(ret.data[0].排名, 1);
  });

  test('select * from 用户 where 性别 is null', () => {
    const ret = session.sql('select * from 用户 where 性别 is null');
    assert.strictEqual(ret.data.length, 1);
    assert.strictEqual(ret.data[0].姓名, '赵六');
  });

  test("select * from 用户 where 姓名 in ('张三', 'lucy')", () => {
    const ret = session.sql("select * from 用户 where 姓名 in ('张三', 'lucy')");
    assert.strictEqual(ret.data.length, 2);
  });

  test('select * from 用户 where 分数 >= 90 and 性别 = \'男\'', () => {
    const ret = session.sql('select * from 用户 where 分数 >= 90 and 性别 = \'男\'');
    assert.strictEqual(ret.data.length, 2);
    assert.ok(ret.data.every(r => r.性别 === '男' && r.分数 >= 90));
  });

  test('select * from 用户 where city = \'北京\' or city = \'上海\'', () => {
    const ret = session.sql('select * from 用户 where city = \'北京\' or city = \'上海\'');
    assert.strictEqual(ret.data.length, 3);
    assert.ok(ret.data.some(r => r.city === '北京'));
    assert.ok(ret.data.some(r => r.city === '上海'));
  });

  test('select * from 用户 where city not in (\'北京\', \'上海\')', () => {
    const ret = session.sql('select * from 用户 where city not in (\'北京\', \'上海\')');
    assert.ok(ret.data.every(r => r.city !== '北京' && r.city !== '上海'));
  });

  test('select * from 用户 where city in (\'北京\', null)', () => {
    const ret = session.sql('select * from 用户 where city in (\'北京\', null)');
    // 只有city为'北京'的行会被选中，city为null不会被选中
    assert.ok(ret.data.every(r => r.city === '北京'));
  });

  test('select * from 用户 where city not in (\'北京\', null)', () => {
    const ret = session.sql('select * from 用户 where city not in (\'北京\', null)');
    // SQL标准：not in列表有null时，结果都不会被选中
    assert.strictEqual(ret.data.length, 0);
  });

  test('select * from 用户 where city is null or 性别 is null', () => {
    const ret = session.sql('select * from 用户 where city is null or 性别 is null');
    assert.strictEqual(ret.data.length, 2);
    assert.ok(ret.data.some(r => r.city === null));
    assert.ok(ret.data.some(r => r.性别 === null));
  });

  test('select count(1) as 总数 from 用户', () => {
    const ret = session.sql('select count(1) as 总数 from 用户');
    assert.strictEqual(ret.data[0].总数, 6);
  });

  test('select 性别, avg(分数) as 平均分 from 用户 group by 性别', () => {
    const ret = session.sql('select 性别, avg(分数) as 平均分 from 用户 group by 性别');
    assert.ok(ret.data.some(r => r.性别 === '男'));
    assert.ok(ret.data.some(r => r.性别 === '女'));
  });

  test('select * from 用户 where 姓名 like \'%o%\'', () => {
    const ret = session.sql('select * from 用户 where 姓名 like \'%o%\'');
    assert.ok(ret.data.some(r => r.姓名 === 'tom'));
  });

  test('select * from 用户 where 姓名 not like \'%三\'', () => {
    const ret = session.sql('select * from 用户 where 姓名 not like \'%三\'');
    assert.ok(ret.data.every(r => r.姓名 !== '张三'));
  });

  test('select * from 用户 where age > 20 order by age asc, 分数 desc', () => {
    const ret = session.sql('select * from 用户 where age > 20 order by age asc, 分数 desc');
    assert.ok(ret.data.length > 0);
    for (let i = 1; i < ret.data.length; i++) {
      assert.ok(ret.data[i].age >= ret.data[i - 1].age);
    }
  });

  test('select * from 用户 where city is not null limit 2', () => {
    const ret = session.sql('select * from 用户 where city is not null limit 2');
    assert.strictEqual(ret.data.length, 2);
  });

  test('select max(分数) as 最高分, min(分数) as 最低分 from 用户', () => {
    const ret = session.sql('select max(分数) as 最高分, min(分数) as 最低分 from 用户');
    assert.strictEqual(ret.data[0].最高分, 95);
    assert.strictEqual(ret.data[0].最低分, 77);
  });
});
