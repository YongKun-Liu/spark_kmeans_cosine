package cluster.discosine.utils

import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created  on 18/06/05.
  */
object TimeUtils {

    def getDayAgoHour(pdate:String, ago: Int): String = {
        val format = "yyyyMMddHH"
        val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
        val date = dateFormat.parse(pdate)
        val calender: Calendar = Calendar.getInstance()
        calender.setTime(date)
        calender.add(Calendar.DAY_OF_MONTH, -1 * ago)
        val time = dateFormat.format(calender.getTime)
        time
    }

    def getHourAgoHour(pdate:String, ago: Int): String = {
        val format = "yyyyMMddHH"
        val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
        val date = dateFormat.parse(pdate)
        val calender: Calendar = Calendar.getInstance()
        calender.setTime(date)
        calender.add(Calendar.HOUR_OF_DAY, -1 * ago)
        val time = dateFormat.format(calender.getTime)
        time
    }

    def getDayAgoTime(pdate:String, ago: Int): String = {
        val format = "yyyyMMddHHmm"
        val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
        val date = dateFormat.parse(pdate)
        val calender: Calendar = Calendar.getInstance()
        calender.setTime(date)
        calender.add(Calendar.DAY_OF_MONTH, -1 * ago)
        val time = dateFormat.format(calender.getTime)
        time
    }

    def getMinuteAgoTime(pdate:String, ago: Int): String = {
        val format = "yyyy/MM/dd/HH/mm"
        val dateFormat: SimpleDateFormat = new SimpleDateFormat(format)
        val date = dateFormat.parse(pdate)
        val calender: Calendar = Calendar.getInstance()
        calender.setTime(date)
        calender.add(Calendar.MINUTE, ago)
        val time = dateFormat.format(calender.getTime)
        time
    }

    def main(args: Array[String]): Unit = {
        println(getHourAgoHour("2018082619", 5 * 24))
        val s = "20180826"
        println(s.substring(0, 4))
        println(s.substring(4, 6))
        println(s.substring(6, 8))

    }
}
