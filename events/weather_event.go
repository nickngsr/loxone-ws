package events

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	log "github.com/sirupsen/logrus"
	"time"

	"github.com/zhuangsirui/binpacker"
)

const LoxEpochOffset = 1230768000

/*
int timestamp; // 32-Bit Integer (little endian)
int weatherType; // 32-Bit Integer (little endian)
int windDirection; // 32-Bit Integer (little endian)
int solarRadiation; // 32-Bit Integer (little endian)
int relativeHumidity; // 32-Bit Integer (little endian)
double temperature; // 64-Bit Float (little endian)
double perceivedTemperature; // 64-Bit Float (little endian)
double dewPoint; // 64-Bit Float (little endian)
double precipitation; // 64-Bit Float (little endian)
double windSpeed; // 64-Bit Float (little endian)
double barometicPressure; // 64-Bit Float (little endian)
*/

type WeatherEventTable struct {
	UUID        string    // 128 bits
	NoOfEntries int32     // 32 bit unsigned in
	LastUpdated time.Time // since 1st jan 2009 32 bit unsigned int
	Entries     []WeatherEvent
}

type WeatherEvent struct {
	Timestamp            time.Time
	WeatherType          int32
	WindDirection        int32
	SolarRadiation       int32
	RelativeHumidity     int32
	Temperature          float64
	PerceivedTemperature float64
	DewPoint             float64
	Precipitation        float64
	WindSpeed            float64
	BarometricPressure   float64
}

func InitWeatherEventTable(data []byte) []WeatherEventTable {
	if len(data) < 24 {
		return nil
	}

	buffer := bytes.NewReader(data)
	up := binpacker.NewUnpacker(binary.LittleEndian, buffer)
	var tables []WeatherEventTable

	for {

		if buffer.Len() < 24 {
			break
		}

		uuidBytes, err := up.ShiftBytes(16)
		if err != nil {
			return nil
		}
		uuid := readUUID(uuidBytes)

		lastUpdated, err := up.ShiftUint32()
		if err != nil {
			return nil
		}

		entries, err := up.ShiftInt32()
		if err != nil {
			return nil
		}

		weatherTable := WeatherEventTable{
			UUID:        uuid,
			LastUpdated: time.Unix(int64(lastUpdated+LoxEpochOffset), 0),
			NoOfEntries: entries,
			Entries:     make([]WeatherEvent, 0, entries),
		}

		for i := int32(0); i < entries; i++ {
			event, err := getWeatherEvent(up)
			if err != nil {
				log.Errorf("Error parsing weather event: %s", err)
				return nil
			}
			weatherTable.Entries = append(weatherTable.Entries, event)
		}

		tables = append(tables, weatherTable)

	}

	return tables

}

func getWeatherEvent(up *binpacker.Unpacker) (WeatherEvent, error) {
	var event WeatherEvent

	loxTimestamp, err := up.ShiftInt32()
	if err != nil {
		return event, err
	}
	event.Timestamp = time.Unix(int64(loxTimestamp+LoxEpochOffset), 0)

	event.WeatherType, err = up.ShiftInt32()
	if err != nil {
		return event, err
	}

	event.WindDirection, err = up.ShiftInt32()
	if err != nil {
		return event, err
	}

	event.SolarRadiation, err = up.ShiftInt32()
	if err != nil {
		return event, err
	}

	event.RelativeHumidity, err = up.ShiftInt32()
	if err != nil {
		return event, err
	}

	event.Temperature, err = up.ShiftFloat64()
	if err != nil {
		return event, err
	}

	event.PerceivedTemperature, err = up.ShiftFloat64()
	if err != nil {
		return event, err
	}

	event.DewPoint, err = up.ShiftFloat64()
	if err != nil {
		return event, err
	}

	event.Precipitation, err = up.ShiftFloat64()
	if err != nil {
		return event, err
	}

	event.WindSpeed, err = up.ShiftFloat64()
	if err != nil {
		return event, err
	}

	event.BarometricPressure, err = up.ShiftFloat64()
	if err != nil {
		return event, err
	}

	return event, nil
}

func getUUID(up *binpacker.Unpacker) (string, error) {
	var uuid string

	sect, err := buildHexString(up, 4, "")
	if err != nil {
		return "", err
	}
	uuid += sect

	sect, err = buildHexString(up, 2, "-")
	if err != nil {
		return "", err
	}
	uuid += sect

	sect, err = buildHexString(up, 2, "-")
	if err != nil {
		return "", err
	}
	uuid += sect

	sect, err = buildHexString(up, 8, "-")
	if err != nil {
		return "", err
	}
	uuid += sect

	return uuid, nil
}

func buildHexString(up *binpacker.Unpacker, bytes uint64, prefix string) (string, error) {
	section, err := up.ShiftBytes(bytes)
	if err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(section), nil
}
