## 此文章仅做个人学习记录与参考，如有错误，欢迎指正
## 博客地址 [ https://blog.csdn.net/qq_31343581/article/category/7764829 ]
# 一、准备
##### 1.  虚拟机：`VMware Fusion`
##### 2. IDE：`Intellij`
###### idea快捷键 [ windows https://blog.csdn.net/deniro_li/article/details/72902621 ]
###### idea快捷键 [ mac  https://blog.csdn.net/as300403/article/details/79216706 ]
##### 3. JDK1.8
##### 4. hadoop2.5.2，参照文档  http://hadoop.apache.org/docs/r2.5.2/index.html
##### 5. zookeeper-3.4.6，参照文档  http://zookeeper.apache.org/doc/r3.4.12/
##### 6. 机器：macbook pro 2017版 8G内存
# 二、虚拟机准备
##### 1. 搭建 4台Centos7，下载的是min版本的`CentOS-7-x86_64-Minimal-1804.iso`
##### 2. 分配静态ip
Centos7中查看ip的命令为`ip a`

![这里写图片描述](https://img-blog.csdn.net/20180628184903386?watermark/2/text/aHR0cHM6Ly9ibG9nLmNzZG4ubmV0L3FxXzMxMzQzNTgx/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70)

编辑文件 `/etc/sysconfig/network-scripts/ifcfg-ens33`
参照https://blog.csdn.net/qq_31343581/article/details/80833912
##### 3. 关闭防火墙和SeLinux
Centos7中默认的防火墙是firewall
https://blog.csdn.net/Post_Yuan/article/details/78603212

	//临时关闭
	systemctl stop firewalld
	//禁止开机启动
	systemctl disable firewalld
	Removed symlink /etc/systemd/system/multi-user.target.wants/firewalld.service.
	Removed symlink /etc/systemd/system/dbus-org.fedoraproject.FirewallD1.service.
##### 4. 修改yum源
建议修改为163的yum源，方法自行百度
##### 5.同步服务器时间
使用ntpdate，网上搜一个时间服务器假设为xxxx.xxxx.xxxx

	ntpdate xxxx.xxxx.xxxx
失败了的话，详细参考这篇文章 [ https://blog.csdn.net/qq_27754983/article/details/69386408 ]
