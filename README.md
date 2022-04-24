# flink-cep-task

#### 介绍
一个简化版的flink CEP引擎，可以动态改变聚合规则，动态计算结果并输出

#### 软件架构
Data Structure in org.apache.sn.task.engine.CEPEngine

#### 使用说明

1. nc -l 8888 （输入rule json）
2. nc -l 9999 （输入metric json）
3. 启动 org.apache.sn.task.CEPTaskRunner
4. 参照 resources/metrics 和 resources/rules 里的样例发送数据给以上两端口

#### 参与贡献

1.  Fork 本仓库
2.  新建 Feat_xxx 分支
3.  提交代码
4.  新建 Pull Request
