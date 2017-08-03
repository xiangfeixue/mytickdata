library(data.table)
library(RMySQL)
library(readr)
library(magrittr)
library(plyr)

gettickdata<-function(filename)
{
  td<- read_csv(filename,col_types="cccccddddddddddddddddddddddddddddddddddddd")
  tickdata1 <- data.table(td)
  return(tickdata1)
}


dealtick<-function(mydt)
{
  
  #head(tickdata)
  
  
  #报价无效
  tickdata1<-mydt[volume!=0&lastPrice<100000000]
  #head(tickdata1)
  
  
  #非交易时段内的
  tickdata2<-tickdata1[time<="02:30:00"|time>="21:00:00"|(time>="09:00:00"&time<="15:15:00")]
  #length(tickdata2[,volume])
  #head(tickdata2)
  
  #交易所时间和tick时间超过1分钟
  tickdata2[, hour := substr(timeStamp, 10,11)]
  tickdata2[,min := substr(timeStamp,13,14)]
  tickdata2[,secs := substr(timeStamp,16,17)]
  tickdata2[,NumericRecvTime := 3600*as.numeric(hour) +60*as.numeric(min) +as.numeric(secs)]
  tickdata2_1<-tickdata2[hour>18]
  
  tickdata2_2<-tickdata2[hour<18]
  
  
  tickdata2_1<-tickdata2_1[,NumericRecvTime:=NumericRecvTime-86400]
  
  #tickdata2<-merge(tickdata2_1,tickdata2_2,all=TRUE)
  tickdata2<-rbind(tickdata2_1,tickdata2_2)
  
  #length(tickdata2[,volume])
  #head(tickdata2)
  #help("rbind")
  
  
  tickdata2[,hour1:=substr(time,1,2)]
  tickdata2[,min1:=substr(time,4,5)]
  tickdata2[,secs1:=substr(time,7,8)]
  tickdata2[,NumericExchTime := 3600*as.numeric(hour1) +60*as.numeric(min1) +as.numeric(secs1)]
  
  tickdata2_1<-tickdata2[hour1>18]
  
  tickdata2_2<-tickdata2[hour1<18]
  
  tickdata2_1<-tickdata2_1[,NumericExchTime:=NumericExchTime-86400]
  
  tickdata2<-rbind(tickdata2_1,tickdata2_2)
  #tickdata2<-merge(tickdata2_1,tickdata2_2,all=TRUE,)
  #length(tickdata2[,volume])
  
  
  tickdata3<-tickdata2[((NumericRecvTime-NumericExchTime)<=60) &  ((NumericRecvTime-NumericExchTime)>=-60),]
  
  
  #head(tickdata3)
  #length(tickdata3[,volume])
  
  #volume或者turnover不是单调递增
  tickdata3<-setorder(tickdata3,symbol,timeStamp)
  
  #tickdata3[,preOpenInterest]<-diff(tickdata3[,volume],1,by=.(symbol))
  
  tickdata3[,DeltaVolume:=c(volume[1],diff(volume)),by=.(symbol)]
  
  
  tickdata3[,DeltaTurnover:=c(turnover[1],diff(turnover)),by=.(symbol)]
  

  
  
  
  tickdata3[,diffaskp:=c(askPrice1[1],diff(askPrice1)),by=.(symbol)]
  
  tickdata3[,diffbidp:=c(bidPrice1[1],diff(bidPrice1)),by=.(symbol)]
  
  
  tickdata4<-tickdata3[!(DeltaVolume==0&DeltaTurnover==0&diffaskp==0&diffbidp==0)]
  #head(tickdata4)
  return(tickdata4[,c(1:42,46,50:52)])
}


#####计算分钟数据
minutedata<-function(alldaytickdata)
{
  alldaytickdata[,exchMinute:=(NumericExchTime%/%60)]
        
  alldaytickdata1<-alldaytickdata[exchMinute>0]
    head(alldaytickdata1)
  alldaytickdata2<-alldaytickdata[exchMinute<=0]
    head(alldaytickdata2)
    return(alldaytickdata)
}



tick2mysql<-function(mytickdata)
{
  mysqlconnect = dbConnect(MySQL(),user = 'xxf',password = 'abc@123',dbname='tickdata',host='192.168.1.166',port=3306)
  dbWriteTable(mysqlconnect,"tickdata",mytickdata,append=TRUE)
  dbDisconnect(mysqlconnect)
  
}