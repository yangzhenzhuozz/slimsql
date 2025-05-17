import resolve from "@rollup/plugin-node-resolve";
export default {
  input: 'dist/main.js', // 入口文件
  output: {
    file: 'dist/main.esm.js', // 输出文件
    format: 'esm', // 输出格式为 ESM
    sourcemap: false, // 生成 sourcemap
  },
  plugins:[resolve()]
};