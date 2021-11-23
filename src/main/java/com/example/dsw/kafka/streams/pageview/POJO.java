package com.example.dsw.kafka.streams.pageview;

public class POJO {
    // POJO classes
    static public class PageView {
        public Long viewtime;
        public String userid;
        public String pageid;
    }

    static public class User {
        public String userid;
        public String regionid;
        public Long registertime;
        public String gender;
    }

    /*TODO ADD YOUR CODE HERE*/
}