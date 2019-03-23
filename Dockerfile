# 构建一个基于 ubuntu 的 ssh 定制镜像
# 基础镜像
FROM ubuntu
# 镜像作者
MAINTAINER  ruiyang0715@gmail.com
# 执行命令
# 增加软件源
ADD sources.list /etc/apt/sources.list
# 安装ssh服务
RUN apt-get update
RUN apt-get install -y openssh-server curl vim net-tools
RUN mkdir -p /var/run/sshd
RUN mkdir -p /root/.ssh
# 取消pam限制
RUN sed -i "s/.*pam_loginuid.so/#&/" /etc/pam.d/sshd
RUN apt-get autoclean
RUN apt-get clean
RUN apt-get autoremove
# 复制配置文件到相应位置,并赋予脚本可执行权限
ADD authorized_keys /root/.ssh/authorized_keys
# 对外端口
EXPOSE 22
# 启动ssh
ENTRYPOINT ["/usr/sbin/sshd","-D"]