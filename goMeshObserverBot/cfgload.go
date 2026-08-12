package main

import (
	"fmt"
	"io/ioutil"
	"log"

	yaml "gopkg.in/yaml.v3"
)

func readCfg() []string {

	fmt.Println("Reading config...")

	var cfgYaml map[string]interface{}
	cfgFile, err := ioutil.ReadFile("config.yml")
	if err != nil {
		log.Fatal(err)
	}

	err = yaml.Unmarshal(cfgFile, &cfgYaml)

	if err != nil {
		log.Fatal(err)
	}

	homeserver := (cfgYaml["matrix"].(map[string]interface{})["homeserver"])
	username := (cfgYaml["matrix"].(map[string]interface{})["username"])
	password := (cfgYaml["matrix"].(map[string]interface{})["password"])
	device_id := (cfgYaml["matrix"].(map[string]interface{})["device_id"])
	target_room := (cfgYaml["room"].(map[string]interface{})["target_room"])
	allowed_user := (cfgYaml["room"].(map[string]interface{})["allowed_user"])

	homeserver_ := fmt.Sprintf("%v", homeserver)
	username_ := fmt.Sprintf("%v", username)
	password_ := fmt.Sprintf("%v", password)
	device_id_ := fmt.Sprintf("%v", device_id)
	target_room_ := fmt.Sprintf("%v", target_room)
	allowed_user_ := fmt.Sprintf("%v", allowed_user)

	var out []string
	out = append(out, homeserver_, username_, password_, device_id_, target_room_, allowed_user_)

	fmt.Println(out)
	return out
}
