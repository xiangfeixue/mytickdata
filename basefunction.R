getmysqldata<-function(mydbname,sqlcommd)
{
  library(RMySQL)
  mysqlconnect1 = dbConnect(MySQL(),user = 'xxf',password = 'abc@123',dbname=mydbname,host='192.168.1.106',port=3306)
  result = dbSendQuery(mysqlconnect1,sqlcommd)
  data = fetch(result,-1)
  data
}

#data<-getmysqldata("china_futures_bar","select TradingDay,InstrumentID,Volume,OpenPrice,ClosePrice from daily where Sector='allday' and TradingDay >= '2017-06-01' order by InstrumentID,TradingDay desc")
#print(data)
myscore<-function(data)
{
  
  len <- length(data[,1])
  instrument_now <-data[1,1]
  index <- 1
  for(i in 1:len)
  {
    if(data[i,1] != instrument_now)
    {
      data[index:i-1,2] <- scale(data[index:i-1,2],TRUE,TRUE)
      index <- i
      instrument_now <- data[i,1]
    }
  }
  data[index:len,2] <- scale(data[index:len,2],TRUE,TRUE)
  data
}

#mydata<-myscore(data[,c(2,4)])
#print(mydata)


getreturn<-function(data,days)
{
  
}