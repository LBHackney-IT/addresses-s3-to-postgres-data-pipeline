using System;
using System.ComponentModel.DataAnnotations;

namespace AddressesDataPipeline.Database
{
    public class CsvUploadRecord
    {
        public long id { get; set; }
        public double uprn { get; set; }
        public double? parent_uprn { get; set; }
        public double? udprn { get; set; }
        public double usrn { get; set; }
        [MaxLength(20)] public string toid { get; set; }
        [MaxLength(6)] public string classification_code { get; set; }
        public double? easting { get; set; }
        public double? northing { get; set; }
        public double? latitude { get; set; }
        public double? longitude { get; set; }
        [MaxLength(10)] public string rpc { get; set; }
        public DateTime? last_update_date { get; set; }
        [MaxLength(800)] public string single_line_address { get; set; }
        [MaxLength(18)] public string po_box { get; set; }
        [MaxLength(100)] public string organisation { get; set; }
        [MaxLength(120)] public string sub_building { get; set; }
        [MaxLength(100)] public string building_name { get; set; }
        [MaxLength(17)] public string building_number { get; set; }
        [MaxLength(100)] public string street_name { get; set; }
        [MaxLength(100)] public string locality { get; set; }
        [MaxLength(100)] public string town_name { get; set; }
        [MaxLength(100)] public string post_town { get; set; }
        [MaxLength(8)] public string island { get; set; }
        [MaxLength(8)] public string postcode { get; set; }
        [MaxLength(8)] public string delivery_point_suffix { get; set; }
        [MaxLength(100)] public string gss_code { get; set; }
        [MaxLength(8)] public string change_code { get; set; }
    }

    public class Address
    {
        [StringLength(14)]
        public string lpi_key { get; set; }

        [MaxLength(18)]
        public string lpi_logical_status { get; set; }
        public int? lpi_start_date { get; set; }
        public int? lpi_end_date { get; set; }
        public int lpi_last_update_date { get; set; }
        public int? usrn { get; set; }
        public long uprn { get; set; }
        public long? parent_uprn { get; set; }
        public int? blpu_start_date { get; set; }
        public int? blpu_end_date { get; set; }
        [MaxLength(4)]
        public string blpu_class { get; set; }
        public int? blpu_last_update_date { get; set; }
        [MaxLength(160)]
        public string usage_description { get; set; }
        [MaxLength(160)]
        public string usage_primary { get; set; }
        public bool property_shell { get; set; }
        public double? easting { get; set; }
        public double? northing { get; set; }
        [MaxLength(17)]
        public string unit_number { get; set; }
        [MaxLength(90)]
        public string sao_text { get; set; }
        [MaxLength(17)]
        public string building_number { get; set; }
        [MaxLength(90)]
        public string pao_text { get; set; }
        public short? paon_start_num { get; set; }
        [MaxLength(100)]
        public string street_description { get; set; }
        [MaxLength(100)]
        public string locality { get; set; }
        [MaxLength(100)]
        public string ward { get; set; }
        [MaxLength(100)]
        public string town { get; set; }
        [MaxLength(8)]
        public string postcode { get; set; }
        [MaxLength(8)]
        public string postcode_nospace { get; set; }
        [MaxLength(50)]
        public string planning_use_class { get; set; }
        public bool neverexport { get; set; }
        public double? longitude { get; set; }
        public double? latitude { get; set; }
        [MaxLength(8)]
        public string gazetteer { get; set; }
        [MaxLength(100)]
        public string organisation { get; set; }
        [MaxLength(200)]
        public string line1 { get; set; }
        [MaxLength(200)]
        public string line2 { get; set; }
        [MaxLength(200)]
        public string line3 { get; set; }
        [MaxLength(100)]
        public string line4 { get; set; }
    }
}