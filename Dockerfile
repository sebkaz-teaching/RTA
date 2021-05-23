# Ubuntu for our image
FROM ubuntu

# Updating Ubuntu
RUN apt-get update && yes | apt-get upgrade

# adding java 8
RUN apt-get install -y openjdk-8-jdk
RUN java -version
# Adding python3 and pip3
RUN apt-get install -y python3 python3-pip
# adding curl and git
#RUN apt-get install -y wget git

# SPARK DOWNLOAD
#RUN wget https://apache.mirrors.tworzy.net/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz

#RUN tar xvf spark-*
#RUN mv spark-3.1.1-bin-hadoop2.7 /opt/spark
# SPARK EXPORTS
#RUN echo "export SPARK_HOME=/opt/spark" >> ~/.profile
#RUN echo "export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin" >> ~/.profile
#RUN echo "export PYSPARK_PYTHON=/usr/bin/python3" >> ~/.profile

#RUN source ~/.profile


RUN pip3 install jupyter pandas numpy matplotlib sklearn pyspark findspark

COPY . /notebooks/

# Configuring access to Jupyter
WORKDIR /notebooks/

# Configuring access to Jupyter

RUN jupyter notebook --generate-config --allow-root
RUN echo "c.NotebookApp.password = u'sha1:6a3f528eec40:6e896b6e4828f525a6e20e5411cd1c8075d68619'" >> /root/.jupyter/jupyter_notebook_config.py

# Add Tini. Tini operates as a process subreaper for jupyter. This prevents kernel crashes.
ENV TINI_VERSION v0.6.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /usr/bin/tini
RUN chmod +x /usr/bin/tini
ENTRYPOINT ["/usr/bin/tini", "--"]

# Jupyter listens port: 8888
EXPOSE 8888

# Run Jupyter notebook as Docker main process
#CMD ["jupyter", "notebook", "--allow-root", "--notebook-dir=/notebooks", "--ip='*'", "--port=8888", "--no-browser"]

# Run Jupyter notebook  without password - not recomended !!!
CMD ["jupyter", "notebook", "--allow-root", "--notebook-dir=/notebooks", "--ip='*'", "--port=8888", "--no-browser", "--NotebookApp.token=''"]
