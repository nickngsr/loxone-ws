package events

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
	"strings"
)

// Event represent a Loxone EventTypeEvent with an UUID and a Value
type Event struct {
	UUID     string
	UUIDIcon string
	Value    float64
	Text     string
}

type BinaryEvent struct {
	EventType EventType
	Events    []Event
	Data      []byte
}

type Header struct {
	EventType EventType
	Length    int
	Estimated bool
	Empty     bool
}

var EmptyHeader = &Header{Empty: true}

type EventType int

const (
	EventTypeText         EventType = 0
	EventTypeFile         EventType = 1
	EventTypeEvent        EventType = 2
	EventTypeEventtext    EventType = 3
	EventTypeDaytimer     EventType = 4
	EventTypeOutofservice EventType = 5
	EventTypeKeepalive    EventType = 6
	EventTypeWeather      EventType = 7
)

func (e *BinaryEvent) readEventText(data []byte) {
	reader := bytes.NewReader(data)
	events := make([]Event, 0)
	for {
		uuidChunk := make([]byte, 36)

		if _, err := reader.Read(uuidChunk); err == io.EOF {
			break
		}

		textSize := binary.LittleEndian.Uint32(uuidChunk[32:36])

		padding := textSize % 4

		nextSize := binary.LittleEndian.Uint32(uuidChunk[32:36])

		if padding > 0 {
			nextSize = nextSize + 4 - padding
		}

		textChunk := make([]byte, nextSize)

		if _, err := reader.Read(textChunk); err == io.EOF {
			break
		}

		events = append(events, Event{
			UUID:     readUUID(uuidChunk[0:16]),
			UUIDIcon: readUUID(uuidChunk[16:32]),
			Text:     string(textChunk[0:textSize]),
		})
	}

	e.Events = events
}

func (e *BinaryEvent) readEvent(data []byte) {
	reader := bytes.NewReader(data)
	// 1 EventTypeEvent = 24 Bytes
	p := make([]byte, 24)
	events := make([]Event, 0)
	for {
		n, err := reader.Read(p)
		if err == io.EOF {
			break
		}
		events = append(events, createEvent(p[:n]))
	}

	e.Events = events
}

func createEvent(eventRaw []byte) Event {
	uuid := readUUID(eventRaw[0:16])
	value := math.Float64frombits(binary.LittleEndian.Uint64(eventRaw[16:24]))
	return Event{
		UUID:  uuid,
		Value: value,
	}
}

func IdentifyHeader(bytes []byte) (*Header, error) {
	if len(bytes) != 8 {
		return nil, errors.New("error: wrong binary Header received")
	}
	eventTypeValue := bytes[1]
	length := int(binary.LittleEndian.Uint32(bytes[4:]))

	estimated := false
	if bytes[2] == 128 {
		estimated = true
	}

	return &Header{
		EventType: EventType(eventTypeValue),
		Length:    length,
		Estimated: estimated,
	}, nil
}

func InitBinaryEvent(bytes []byte, eventType EventType) *BinaryEvent {
	binaryEvent := &BinaryEvent{EventType: eventType}

	switch eventType {
	case EventTypeEventtext:
		binaryEvent.readEventText(bytes)
	case EventTypeEvent:
		binaryEvent.readEvent(bytes)
	case EventTypeDaytimer:
		// TODO
	}

	return binaryEvent
}

func readUUID(data []byte) string {
	values := []string{
		extract32Bytes(data[0:4]),
		extract16Bytes(data[4:6]),
		extract16Bytes(data[6:8]),
		extract64Bytes(data[8:16]),
	}

	return strings.Join(values, "-")
}

func extract16Bytes(data []byte) string {
	b := make([]byte, len(data))
	u := binary.LittleEndian.Uint16(data)
	binary.BigEndian.PutUint16(b, u)
	return hex.EncodeToString(b)
}
func extract32Bytes(data []byte) string {
	b := make([]byte, len(data))
	u := binary.LittleEndian.Uint32(data)
	binary.BigEndian.PutUint32(b, u)
	return hex.EncodeToString(b)
}
func extract64Bytes(data []byte) string {
	return hex.EncodeToString(data)
}
