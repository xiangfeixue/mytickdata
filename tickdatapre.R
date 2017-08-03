source("/home/xxf/tickfunction.R")
library(data.table)
library(RMySQL)
library(readr)
library(magrittr)

#日盘数据读入
daytime = "20170704";

#head(calendardata)
dayfileroad <- paste("/shared/public/fl/Tick/",daytime,".csv",sep="")
#fileroad
#help(read_csv)

#读取数据
tickdata<-gettickdata(dayfileroad)
#数据处理
tickdata[,hour:=as.numeric(substr(time,1,2))]

tickdata<-tickdata[(hour>=9)&(hour<16)]

mydaytickdata <- dealtick(tickdata)

#mytickdata



#获取夜盘数据
cd<-getmysqldata("dev","select * from ChinaFuturesCalendar");
#head(calendar)
calendardata <- data.table(cd)

year<-substr(daytime,1,4)
month<-substr(daytime,5,6)
day<-substr(daytime,7,8)

daycalendar<-paste(year,month,day,sep="-")
getdata<-calendardata[days==daycalendar]
nightcalendar<-getdata[,nights]


if(!is.na(nightcalendar))
{
  year1<-substr(nights,1,4)
  month1<-substr(nights,6,7)
  day1<-substr(nights,9,10)
  nighttime<-paste(year1,month1,day1,sep="")
  nightfileroad <- paste("/shared/public/fl/Tick/",nighttime,".csv",sep="")
  
  #读取数据
  nighttickdata<-gettickdata(nightfileroad)
  nighttickdata[,hour:=as.numeric(substr(time,1,2))]
  nighttickdata<-nighttickdata[(hour>=21)|(hour<3)]
  #数据处理
  mynighttickdata <- dealtick(nighttickdata)
  #mytickdata
  alldaytickdata<-rbind(mynighttickdata,mydaytickdata)
}else{
  alldaytickdata<-mydaytickdata
}
setorder(alldaytickdata,symbol,timeStamp)
#存入数据库
tick2mysql(alldaytickdata)

#计算minute数据
#alldayminutedata <- minutedata(alldaytickdata)
alldaytickdata[,exchMinute:=(NumericExchTime%/%60)]

alldaytickdata[,exchMinute:=ifelse(exchMinute<=0,exchMinute+1440,exchMinute+0)]

alldaytickdata[,exchhour:=as.character(exchMinute%/%60)]
alldaytickdata[,exchminute:=as.character(exchMinute%%60)]

alldaytickdata[,exchhour:=ifelse(as.numeric(alldaytickdata$exchhour)<10,paste("0",alldaytickdata$exchhour,sep=""),alldaytickdata$exchhour)]
alldaytickdata[,exchminute:=ifelse(as.numeric(exchminute)<10,paste("0",exchminute,sep=""),exchminute)]

alldaytickdata[,Minute:=paste(exchhour,exchminute,"00",sep=":")]

alldaytickdata[,.(OpenPrice = .SD[1,lastPrice], HighPrice = max(.SD$lastPrice),LowPrice = min(.SD$lastPrice),ClosePrice = .SD[.N,lastPrice]),by = c('symbol','Minute')]


#计算breaktime数据

temp <- alldaytickdata[symbol=='i1709']

