
# flink demo

## 安装

使用docker-compose。

## 快速开始

link Maven Archetype 开始快速使用包含 flink程序的骨架。
mvn archetype:generate \
-DarchetypeGroupId=org.apache.flink \
-DarchetypeArtifactId=flink-walkthrough-datastream-java \
-DarchetypeVersion=1.17-SNAPSHOT \
-DgroupId=frauddetection \
-DartifactId=frauddetection \
-Dversion=0.1 \
-Dpackage=spendreport \
-DinteractiveMode=false

需要增加 mvn 配置到 `settings.xml`：

```
<settings>
  <activeProfiles>
    <activeProfile>apache</activeProfile>
  </activeProfiles>
  <profiles>
    <profile>
      <id>apache</id>
      <repositories>
        <repository>
          <id>apache-snapshots</id>
          <url>https://repository.apache.org/content/repositories/snapshots/</url>
        </repository>
      </repositories>
    </profile>
  </profiles>
</settings>
```

注意：在IDE运行，需要更新IDE配置：`Build and Run`右侧调整`modify option`，钩上 `add dependencies with provided scope to classpath`。