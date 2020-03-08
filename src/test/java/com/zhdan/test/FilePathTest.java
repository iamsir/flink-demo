package com.zhdan.test;

/**
 * @author zhdan
 * @date 2020-03-07
 */
public class FilePathTest {

    public static void main(String[] args) {

        String filePath = FilePathTest.class.getClassLoader().getResource("").getPath();

        System.out.println(filePath);

    }
}
