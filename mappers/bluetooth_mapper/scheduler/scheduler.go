/*
Copyright 2019 The KubeEdge Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package scheduler

import (
	"fmt"
	"strings"
	"time"

	"k8s.io/klog/v2"

	"github.com/kubeedge/kubeedge/mappers/bluetooth_mapper/action_manager"
	"github.com/kubeedge/kubeedge/mappers/bluetooth_mapper/data_converter"
	"github.com/kubeedge/kubeedge/mappers/bluetooth_mapper/helper"
)

const (
	MapperTopicPrefix     = "$ke/device/bluetooth-mapper/"
	SchedulerResultSuffix = "/scheduler/result"
	defaultEventFrequency = 5000
)

// Schedule is structure to define a schedule
type Schedule struct {
	// Name is name of the schedule. It should be unique so that a stop-chan
	// can be made corresponding to name to stop the schedule.
	Name string `yaml:"name" json:"name"`
	// Interval is the time in milliseconds after which this action are to be performed
	Interval int `yaml:"interval" json:"interval"`
	//OccurrenceLimit refers to the number of time the action can occur, if it is 0, then the  event will execute infinitely
	OccurrenceLimit int `yaml:"occurrence-limit" json:"occurrence-limit"`
	// Actions is list of Actions to be performed in this schedule
	Actions []string `yaml:"actions"`
}

//Scheduler structure contains the list of schedules to be scheduled
type Scheduler struct {
	Schedules []Schedule `yaml:"schedules" json:"schedules"`
}

//ScheduleResult structure contains the format in which telemetry data will be published on the MQTT topic
type ScheduleResult struct {
	EventName   string
	TimeStamp   int64
	EventResult string
}

// ExecuteSchedule is responsible for scheduling the operations
func (schedule *Schedule) ExecuteSchedule(actionManager []actionmanager.Action, dataConverter dataconverter.DataRead, deviceID string, deviceName string) {
	klog.Infof("Executing schedule: %s", schedule.Name)
	if schedule.OccurrenceLimit != 0 {
		klog.Infof("Perform time is %s", schedule.OccurrenceLimit)
		for iteration := 0; iteration < schedule.OccurrenceLimit; iteration++ {
			schedule.performScheduleOperation(actionManager, dataConverter, deviceID, deviceName)
		}
	} else {
		klog.Infof("%s perform infinitely.", schedule.Name)
		for {
			schedule.performScheduleOperation(actionManager, dataConverter, deviceID, deviceName)
		}
	}
	//helper.ControllerWg.Done()
}

// performScheduleOperation is responsible for performing the operations associated with the schedule
func (schedule *Schedule) performScheduleOperation(actionManager []actionmanager.Action, dataConverter dataconverter.DataRead, deviceID string, deviceName string) {
	var scheduleResult ScheduleResult
	actionExists := false
	for _, actionName := range schedule.Actions {
		for _, action := range actionManager {
			if strings.EqualFold(action.Name, actionName) {
				actionExists = true
				//klog.Infof("Performing scheduled operation: %s", action.Name)
				action.PerformOperation(dataConverter)
				scheduleResult.EventName = actionName
				scheduleResult.TimeStamp = time.Now().UnixNano() / 1e6
				scheduleResult.EventResult = fmt.Sprintf("%s", action.Operation.Value)
				publishScheduleResult(scheduleResult, deviceID, action.PropertyName, deviceName)
			}
		}
		if schedule.Interval == 0 {
			schedule.Interval = defaultEventFrequency
		}
		if !actionExists {
			klog.Errorf("Action %s does not exist. Exiting from schedule !!!", actionName)
			break
		}
		time.Sleep(time.Duration(3000) * time.Millisecond)
	}
}

//publishScheduleResult publishes the telemetry data on the given MQTT topic
func publishScheduleResult(scheduleResult ScheduleResult, deviceID string, propertyName string, deviceName string) {
	//scheduleResultTopic := MapperTopicPrefix + deviceID + SchedulerResultSuffix
	////klog.Infof("Publishing schedule: %s result on topic: %s", scheduleResult.EventName, scheduleResultTopic)
	//scheduleResultBody, err := json.Marshal(scheduleResult)
	//if err != nil {
	//	klog.Errorf("Error: %s", err)
	//}
	//helper.TokenClient = helper.Client.Publish(scheduleResultTopic, 0, false, scheduleResultBody)
	//if helper.TokenClient.Wait() && helper.TokenClient.Error() != nil {
	//	klog.Errorf("client.publish() Error in device twin get  is %s", helper.TokenClient.Error())
	//}

	//ToCloudTopic := "$hw/events/device/sensor-tag-instance-01/twin/update"
	//ToCloudMessage := helper.CreateActualUpdateMessage2(scheduleResult.EventResult)
	//ToCloudBody, err := json.Marshal(ToCloudMessage)
	//if err != nil {
	//	klog.Errorf("Error: %v", err)
	//}
	//helper.TokenClient = helper.Client.Publish(ToCloudTopic, 0, false, ToCloudBody)
	//if helper.TokenClient.Wait() && helper.TokenClient.Error() != nil {
	//	klog.Errorf("client.publish() Error in device twin get  is %s", helper.TokenClient.Error())
	//}

	text := fmt.Sprintf("{\"__name__\": \"%s\",\"deviceID\": \"%s\",\"%s\": %s}",
		strings.Replace(strings.Replace(deviceName, "-", "_", -1), ".", "_", -1),
		strings.Replace(strings.Replace(deviceID, "-", "_", -1), ".", "_", -1), propertyName, scheduleResult.EventResult)
	klog.Info(text)
	helper.TokenClient = helper.FluentbitClient.Publish("toFluentbit", 0, false, text)
	if helper.TokenClient.Wait() && helper.TokenClient.Error() != nil {
		klog.Errorf("client.publish() Error in device twin get is %s", helper.TokenClient.Error())
	}

}
