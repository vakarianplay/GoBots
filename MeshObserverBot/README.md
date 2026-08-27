# GoMeshObserverBot <img src="https://github.com/meshtastic/firmware/raw/develop/.github/meshtastic_logo.png" alt="Meshtastic Logo" width="80"/> <img src="https://cdn.jsdelivr.net/gh/selfhst/icons/webp/meshcore.webp" alt="Meshtastic Logo" width="80"/>

![alt text](https://img.shields.io/badge/Golang-1.21.1-blue?style=flat-square&logo=go)
![alt text](https://img.shields.io/badge/Matrix%20bot-gray?style=flat-square&logo=matrix)
![alt text](https://img.shields.io/badge/Support-meshcoretel.ru-darkblue?style=flat-square&logo=traefikmesh)
![alt text](https://img.shields.io/badge/Support-onemesh.ru-darkblue?style=flat-square&logo=traefikmesh)

![alt text](https://img.shields.io/badge/Status-in%20complete-2E8B57?style=for-the-badge&logo=Buddy)

### Bot-reporter for matrix in one execute file
Fetching information about mesh-nodes from [meshcoretel.ru](meshcoretel.ru) and [onemesh.ru](onemesh.ru) and send report messages.

<img width="400" alt="изображение" src="https://github.com/user-attachments/assets/c74ae78e-e425-4a13-9052-ce98bb590b60" />
<img width="400" alt="изображение" src="https://github.com/user-attachments/assets/337e58e7-5095-4035-b8f4-a45261a8072a" />



-------------------------

## 🛠️ Releases: 

> For windows x64 and linux x64, armv7, arm64, riscv: [https://github.com/vakarianplay/Gosling_tgbot/releases](https://github.com/vakarianplay/GoBots/releases/tag/MeshObserver)


## 💎 Features

>* Collect from [meshcoretel.ru](meshcoretel.ru) and [onemesh.ru](onemesh.ru)
>* Sendind rich report
>* Work with any matrix room
>* Setiings allowed users
>* Data save in SQLite DB
>* Work with many users
>* Protection from ban

## 🚀 How to start

>* [Download release for your platform](https://github.com/vakarianplay/GoBots/releases/tag/MeshObserver)
>* Unzip archive
>* Edit [config.yml](https://github.com/vakarianplay/GoBots/blob/main/MeshObserverBot/config.yml) for your configuration
>* Run execute

## 📳 Commands

>* help
>* ping
>* !add meshcoretel <ID>
>* !add onemesh <ID>
>* !list meshcoretel
>* !list onemesh
>* !delete meshcoretel <PRIMARY_KEY_ID>
>* !delete onemesh <PRIMARY_KEY_ID>
>* !show meshcoretel
>* !show onemesh
>* !show all


## 📑 Dependencies

>* sqlite: modernc.org/sqlite
>* yaml: gopkg.in/yaml.v3
>* mautrix: maunium.net/go/mautrix


---------------------------------------

## 🔧 Build

>* `go build .`

