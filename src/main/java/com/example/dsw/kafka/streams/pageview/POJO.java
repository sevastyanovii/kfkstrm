package com.example.dsw.kafka.streams.pageview;

public class POJO {
    // POJO classes
    static public class PageView implements PageViewTypedDemo.JSONSerdeCompatible {
        public Long viewtime;
        public String userid;
        public String pageid;
    }

    static public class User implements PageViewTypedDemo.JSONSerdeCompatible {
        public String userid;
        public String regionid;
        public Long registertime;
        public String gender;
    }

    static public class PageViewByGender implements PageViewTypedDemo.JSONSerdeCompatible {
        public PageViewByGender(String userid, String pageid, String gender) {
            this.userid = userid;
            this.pageid = pageid;
            this.gender = gender;
        }

        public String userid;
        public String pageid;
        public String gender;
    }

    static public class WindowedPageViewByGender implements PageViewTypedDemo.JSONSerdeCompatible {
        public long windowStart;
        public String gender;
        public String windowStartString;
    }

    static public class GenderCount implements PageViewTypedDemo.JSONSerdeCompatible {
        public long count;
        public String gender;
    }
}