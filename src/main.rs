use mongodb::bson::doc;
use mongodb::bson::oid::ObjectId;
use mongodb::sync::Client;
use serde::{Deserialize, Serialize};

fn main() {
    const PROVINCE: &str = include_str!("../province.json");
    const CITY: &str = include_str!("../city.json");
    const COUNTY: &str = include_str!("../county.json");

    #[derive(Debug, Deserialize, Serialize)]
    pub struct Province {
        #[serde(rename = "_id")]
        id: ObjectId,
        province_name: String,
        province_code: String,
    }

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    pub struct ProvinceJson {
        name: String,
        code: String,
        id: String,
    }

    impl ProvinceJson {
        fn to_province(&self) -> Province {
            Province {
                id: ObjectId::new(),
                province_name: self.name.clone(),
                province_code: self.code.clone(),
            }
        }
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct City {
        #[serde(rename = "_id")]
        id: ObjectId,
        city_name: String,
        city_code: String,
        province_id: ObjectId,
    }

    impl CityJson {
        fn to_city(&self, province_id: ObjectId) -> City {
            City {
                id: ObjectId::new(),
                city_name: self.name.clone(),
                city_code: self.code.clone(),
                province_id,
            }
        }
    }

    #[derive(Debug, Deserialize, Serialize)]
    pub struct County {
        #[serde(rename = "_id")]
        id: ObjectId,
        county_name: String,
        county_code: String,
        city_id: ObjectId,
    }

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    pub struct CityJson {
        name: String,
        code: String,
        id: String,
        province: String,
    }

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    pub struct CountyJson {
        name: String,
        code: String,
        id: String,
        city: String,
    }

    impl CountyJson {
        fn to_county(&self, city_id: ObjectId) -> County {
            County {
                id: ObjectId::new(),
                county_name: self.name.clone(),
                county_code: self.code.clone(),
                city_id,
            }
        }
    }

    let client = Client::with_uri_str("mongodb://localhost:27017").unwrap();
    let database = client.database("pure-fish");
    let province_collection = database.collection::<Province>("provinces");
    let city_collection = database.collection::<City>("cities");
    let county_collection = database.collection::<County>("counties");

    let province_json_vec: Vec<ProvinceJson> = serde_json::from_str(PROVINCE).unwrap();
    let city_json_vec: Vec<CityJson> = serde_json::from_str(CITY).unwrap();
    let county_json_vec: Vec<CountyJson> = serde_json::from_str(COUNTY).unwrap();

    let province_vec: Vec<Province> = province_json_vec
        .iter()
        .map(|province_json| province_json.to_province())
        .collect();

    let insert_provinces_result = province_collection.insert_many(province_vec, None);

    match insert_provinces_result {
        Ok(_insert_provinces) => {
            let mut province_cursor = province_collection.find(None, None).unwrap();
            let mut province_vec: Vec<Province> = Vec::new();

            while let Some(province) = province_cursor.next() {
                province_vec.push(province.unwrap());
            }

            let mut city_vec: Vec<City> = Vec::new();
            province_vec.iter().for_each(|province| {
                let target_province_json = province_json_vec
                    .iter()
                    .find(|province_json| province_json.name == province.province_name)
                    .unwrap();

                city_json_vec
                    .iter()
                    .filter(|city_json| city_json.province == target_province_json.id)
                    .for_each(|city_json| {
                        let city = city_json.to_city(province.id.clone());
                        city_vec.push(city);
                    });
            });

            let insert_cities_result = city_collection.insert_many(city_vec, None);

            match insert_cities_result {
                Ok(_insert_cities) => {
                    let mut city_cursor = city_collection.find(None, None).unwrap();
                    let mut city_vec: Vec<City> = Vec::new();

                    while let Some(city) = city_cursor.next() {
                        city_vec.push(city.unwrap());
                    }

                    let mut county_vec: Vec<County> = Vec::new();
                    city_vec.iter().for_each(|city| {
                        let target_city_json = city_json_vec
                            .iter()
                            .find(|city_json| city_json.name == city.city_name)
                            .unwrap();

                        county_json_vec
                            .iter()
                            .filter(|county_json| county_json.city == target_city_json.id)
                            .for_each(|county_json| {
                                let county = county_json.to_county(city.id.clone());
                                county_vec.push(county);
                            });
                    });

                    let insert_counties_result = county_collection.insert_many(county_vec, None);

                    match insert_counties_result {
                        Ok(_insert_counties) => {
                            println!("insert counties success");
                        }
                        Err(error) => {
                            println!("insert counties failed: {:#?}", error);
                        }
                    }
                }
                Err(error) => println!("insert cities error: {:#?}", error),
            }
        }
        Err(error) => {
            println!("insert_provinces_result: {:#?}", error);
        }
    }
}
