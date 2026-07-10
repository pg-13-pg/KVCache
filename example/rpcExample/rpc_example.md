# rpc_example

这个示例演示 caller 通过自定义 RPC 框架，远程调用 callee 发布的 `GetFriendsList` 方法。

相关文件：
- `friend.proto`：定义请求、响应和 RPC 服务。
- `caller/callFriendService.cpp`：客户端调用方。
- `callee/friendService.cpp`：服务端被调用方。
- `src/rpc/mprpcchannel.cpp`：caller 侧 RPC 通道。
- `src/rpc/rpcprovider.cpp`：callee 侧 RPC 服务发布和分发。

## 基本流程

1. 编写 `friend.proto`：

```proto
service FiendServiceRpc {
    rpc GetFriendsList(GetFriendsListRequest) returns(GetFriendsListResponse);
}
```

2. 生成 protobuf 代码：

```bash
protoc friend.proto --cpp_out=.
```

3. callee 继承并实现服务：

```cpp
class FriendService : public fixbug::FiendServiceRpc {
  void GetFriendsList(...request, ...response, ...done) override;
};
```

4. callee 注册并启动 RPC 服务：

```cpp
RpcProvider provider;
provider.NotifyService(new FriendService());
provider.Run(1, 7788);
```

5. caller 创建 stub 并发起调用：

```cpp
//FiendServiceRpc_Stub是.proto中对应的service FiendServiceRpc的客户端代理类
fixbug::FiendServiceRpc_Stub stub(new MprpcChannel(ip, port, true));
stub.GetFriendsList(&controller, &request, &response, nullptr);  
```

## 调用链路

完整调用链：

```text
caller
  -> stub.GetFriendsList(...)
      -> FiendServiceRpc_Stub::GetFriendsList(...)
      -> channel_->CallMethod(...)  // 由 protoc 生成的 Stub 内部调用
  -> MprpcChannel::CallMethod(...)
      -> 序列化 request
      -> 构造 RpcHeader(service_name, method_name, args_size)
      -> 组包: header_size + header_str + args_str
      -> socket send

callee
  -> RpcProvider provider
  -> provider.NotifyService(new FriendService())
      -> 注册 service_name / method_name
      -> 保存到 m_serviceMap
  -> provider.Run(1, 7788)
      -> 创建 TcpServer
      -> 监听端口
      -> 注册 OnConnection / OnMessage
      -> 进入 event loop
  -> RpcProvider::OnMessage(...)
      -> 处理 caller 请求
      -> 解析 header_size
      -> 解析 RpcHeader
      -> 根据 service_name 找到 FriendService
      -> 根据 method_name 找到 GetFriendsList
      -> 反序列化 request
      -> 创建 response
      -> service->CallMethod(...)//FriendService类 -> 继承来的 fixbug::FiendServiceRpc::CallMethod()
          -> FriendService::GetFriendsList(...)// FriendService重载方法，故会调用重载后的FriendService的方法
              -> 执行业务逻辑
              -> 填充 response
              -> done->Run() 
                  -> RpcProvider::SendRpcResponse(...)
                      -> 序列化 response
                      -> conn->send(response_str)

caller
  -> MprpcChannel::CallMethod(...) recv response_str //阻塞等待，同步PRC
  -> 反序列化到 response
  -> caller 读取 response
```

`fixbug::FiendServiceRpc_Stub` 是 `protoc` 根据 `friend.proto` 生成的客户端代理类。它的构造函数接收 `google::protobuf::RpcChannel*`，而 `MprpcChannel` 继承了 `google::protobuf::RpcChannel`：

```cpp
class MprpcChannel : public google::protobuf::RpcChannel
```

所以这里可以把 `MprpcChannel` 传给 stub：

```cpp
fixbug::FiendServiceRpc_Stub stub(new MprpcChannel(ip, port, true));
```

stub 内部保存这个通道。之后调用：

```cpp
stub.GetFriendsList(&controller, &request, &response, nullptr);
```

实际会进入生成代码中的：

```cpp
channel_->CallMethod(descriptor()->method(0),
                     controller,
                     request,
                     response,
                     done);
```

因为 `channel_` 实际指向 `MprpcChannel`，所以最终执行的是 `MprpcChannel::CallMethod(...)`，由它完成请求序列化、网络发送、接收响应和反序列化。

## RPC 请求包格式

框架发送的数据格式：

```text
header_size + header_str + args_str
```

含义：

- `header_size`：`header_str` 的长度。
- `header_str`：序列化后的 `RpcHeader`。
- `args_str`：序列化后的业务 request。

`RpcHeader` 中保存：

```text
service_name
method_name
args_size
```

服务端收到请求后，靠 `service_name + method_name` 找到本地方法并调用。

## caller 和 callee 分工

caller：

- 构造 request。
- 通过 stub 调用远程方法。
- 检查 `controller.Failed()` 判断 RPC 框架是否失败。
- 读取 response 中的业务结果。

callee：

- 实现 protobuf 生成的 service 基类。
- 注册服务到 `RpcProvider`。
- 在 RPC 方法中填充 response。
- 调用 `done->Run()` 把结果发回 caller。

RPC 框架：

- 负责 request/response 序列化和反序列化。
- 负责请求头封装和解析。
- 负责 socket 网络发送和接收。
- 负责根据 service/method 分发到本地服务方法。
