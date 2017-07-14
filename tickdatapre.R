source("/home/xxf/basefunction.R")
library(data.table)
library(RMySQL)
library(readr)

#数据读入
daytime = "20170704";

cd<-getmysqldata("dev","select * from ChinaFuturesCalendar");
#head(calendar)
calendardata <- data.table(cd)
#head(calendardata)
dayfileroad <- paste("/shared/public/fl/Tick/",daytime,".csv",sep="")
#fileroad
#help(read_csv)
td<- read_csv(dayfileroad,col_types="cccccddddddddddddddddddddddddddddddddddddd")
tickdata <- data.table(td)
tickdata<-setorder(tickdata,symbol,timeStamp)
#head(tickdata)


#报价无效
tickdata1<-tickdata[volume!=0&lastPrice<100000000]
#head(tickdata1)


#非交易时段内的
tickdata2<-tickdata1[time<="02:30:00"|time>="21:00:00"|(time>="09:00:00"&time<="15:15:00")]
#length(tickdata2[,volume])
#head(tickdata2)

#交易所时间和tick时间超过1分钟
tickdata2[, hour := substr(timeStamp, 10,11)]
tickdata2[,min := substr(timeStamp,13,14)]
tickdata2[,secs := substr(timeStamp,16,17)]
tickdata2[,formatsecs := 3600*as.numeric(hour) +60*as.numeric(min) +as.numeric(secs)]
tickdata2_1<-tickdata2[hour>18]

tickdata2_2<-tickdata2[hour<18]


tickdata2_1<-tickdata2_1[,formatsecs:=formatsecs-86400]

tickdata2<-merge(tickdata2_1,tickdata2_2,all=TRUE)
#length(tickdata2[,volume])
#head(tickdata2)



tickdata2[,hour1:=substr(time,1,2)]
tickdata2[,min1:=substr(time,4,5)]
tickdata2[,secs1:=substr(time,7,8)]
tickdata2[,formatsecs1 := 3600*as.numeric(hour1) +60*as.numeric(min1) +as.numeric(secs1)]

tickdata2_1<-tickdata2[hour1>18]

tickdata2_2<-tickdata2[hour1<18]

tickdata2_1<-tickdata2_1[,formatsecs1:=formatsecs1-86400]
tickdata2<-merge(tickdata2_1,tickdata2_2,all=TRUE)
#length(tickdata2[,volume])

tickdata3<-tickdata2[abs(formatsecs-formatsecs1)<=60]

head(tickdata3)
length(tickdata3[,volume])

#volume或者turnover不是单调递增

tickdata4 <- tickdata3[2:length(tickdata3[volume]),!(diff(volume,1)==0&diff(turnover,1)==0&diff(bidPrice1,1)&diff(askPrice1,1)==0)]
tickdata4 <- tickdata3[diff(volume,1)==0]
length(tickdata4[,volume])
head(tickdata4)

diffvolume<-diff(tickdata3[,volume],1)
tickdata3[,diffvolume:=c(1,diffvolume)]

diffturnover<-diff(tickdata3[,turnover],1)
tickdata3[,diffturnover:=c(1,diffturnover)]

diffaskp<-diff(tickdata3[,askPrice1],1)
tickdata3[,diffaskp:=c(1,diffaskp)]

diffbidp<-diff(tickdata3[,bidPrice1],1)
tickdata3[,diffbidp:=c(1,diffbidp)]

tickdata4<-tickdata3[!(diffvolume==0&diffturnover==0&diffaskp==0&diffbidp==0)]
#head(tickdata4)

#存入数据库
mysqlconnect = dbConnect(MySQL(),user = 'xxf',password = 'abc@123',dbname='tickdata',host='192.168.1.106',port=3306)
dbWriteTable(mysqlconnect,"tickdata",tickdata4[,1:42],append=TRUE)

#计算minute数据

#计算daily数据

#计算breaktime数据

dbDisconnect(mysqlconnect)
