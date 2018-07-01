package com.hadoop.learn;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class Main{

    FileSystem fs;

    Configuration conf;

    @Before
    public void begin() throws IOException {
        // 加载src目录下的配置文件
        conf = new Configuration();

        fs = FileSystem.get(conf);
    }

    @After
    public void end() {
        try {
            fs.close();
        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    // 创建目录
    @Test
    public void mkdir() throws IOException {
        Path path = new Path("/tmp");
        fs.mkdirs(path);
    }

    // 上传
    @Test
    public void upload() throws IOException {
        Path path = new Path("/tmp/test");
        FSDataOutputStream outputStream =  fs.create(path, false);
        FileUtils.copyFile(new File("/Users/zhengyifan/app/testupload"), outputStream);
    }

    @Test
    public void ls() throws IOException {
        Path path = new Path("/tmp");
        FileStatus[] fileStatus = fs.listStatus(path);
        for (FileStatus s : fileStatus) {
            System.out.println(s.getPath() + "-" + s.getLen());
        }
    }

    @Test
    public void upload2() throws IOException {
        Path path = new Path("/tmp/seq");
        SequenceFile.Writer writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
        File file = new File("/Users/zhengyifan/app/test");
        for (File f : file.listFiles()) {
            writer.append(new Text(f.getName()), new Text(FileUtils.readFileToString(f)));
        }
    }

    @Test
    public void download() throws IOException {
        Path path = new Path("/tmp/seq");
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text key = new Text();
        Text value = new Text();
        while (reader.next(key, value)) {
            System.out.println("key--" + key);
            System.out.println("value--" + value);
            System.out.println("----");
        }
    }
}