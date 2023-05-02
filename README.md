# nacosserver

## 如何使用

#### 配置文件调整

```yaml
bootstrap:
  # Global log
  logger:
    nacos-apiserver:
      rotateOutputPath: log/runtime/nacos-apiserver.log
      errorRotateOutputPath: log/runtime/nacos-apiserver-error.log
      rotationMaxSize: 100
      rotationMaxBackups: 10
      rotationMaxAge: 7
      outputLevel: info
      # outputPaths:
      # - stdout
      # errorOutputPaths:
      # - stderr
apiservers:
  - name: service-nacos
    option:
      listenIP: "0.0.0.0"
      listenPort: 8848
      connLimit:
        openConnLimit: false
        maxConnPerHost: 128
        maxConnLimit: 10240
```


## 其他

- NACOS 中的 struct 数据结构定义大部份引用自 [nacos-sdk-go](https://github.com/nacos-group/nacos-sdk-go)