# flink-cep-task

#### 介绍
一个简化版的flink CEP引擎，可以动态改变聚合规则，动态计算结果并输出

#### 软件架构
1. Data Structure 关系在 org.apache.sn.task.engine.CEPEngine 的注释里有提到
2. CEPEngine的设计思路
   1. 职责：数据处理主流程，串联起 **WindowAssigner/Window/Trigger**等组件
   2. 使用flink broadcast特性将rule分发至所有task，以便提取里面的配置
   3. metrics stream按所有tag的value组成metrics的“特征”，按“特征”keyBy，保证同一group的数据一定会分发到同一个task
   4. rule的状态维护在BroadcastState里
3. WindowAssigner的设计思路 
   1. 职责：为metric分配合适的窗口列表（滑动窗口类型下，可能会给一条metric分配多个窗口）
   2. WindowAssigner 负责维护Window的生命周期，并给Window匹配适当的Trigger（Trigger负责窗口的计算触发），以及给metric分配合适的Window
   3. WindowAssigner 还会维护一个originValues，用于计算AVG等需要历史数据的聚合函数时使用。放在WindowAssigner而不是Window里是因为同一WindowAssigner创建的Window可以共用 originValues，以节省内存资源
      1. originValues使用TreeMap，Key按eventTime从小到大排序，方便每个Window按窗口时间段取到自己的数据
4. Window的设计思路
   1. 职责：规定时间窗口的大小和窗口的计算方式，对窗口内的数据进行正确的聚合
   2. 无论什么窗口类型，对于单一窗口来说，都应该可以简化成起止时间和聚合类型两个变量，与WindowAssigner和Trigger逻辑应于窗口本身正交解耦
5. TriggerCenter/Trigger的设计思路
   1. 职责：按时间每一毫秒轮训，得到到期的窗口并触发窗口计算
   2. Trigger随窗口创建而创建，并向TriggerCenter注册。后者使用SingleThreadScheduledExecutor轮训Trigger，及时触发计算

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
