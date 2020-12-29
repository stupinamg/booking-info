package app.entity

class HotelInfo(val name: String,
                val country: String,
                val city: String,
                val address: String,
                val latitude: String,
                val longitude: String,
                val geohash: String) extends Serializable
