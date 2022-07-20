package config

/**
  @author: chenxi@cpgroup.cn
  @date:2022/7/20
  @description:cpx配置
**/

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/nacos-group/nacos-sdk-go/clients"
	"github.com/nacos-group/nacos-sdk-go/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/common/constant"
	"github.com/nacos-group/nacos-sdk-go/common/logger"
	"github.com/nacos-group/nacos-sdk-go/vo"
	"github.com/spf13/viper"
	"io"
	"strings"
)

/**
  @author: chenxi@cpgroup.cn
  @date:2022/6/22
  @description:
**/

type viperConfigManager interface {
	Get(key string) ([]byte, error)
	Watch(key string, stop chan bool) <-chan *viper.RemoteResponse
}

type Config struct {
	DataId string `mapstructure:"dataId"`
}

type Auth struct {
	Enable   bool   `mapstructure:"enable"`
	User     string `mapstructure:"username"`
	Password string `mapstructure:"password"`

	// ACM Endpoint
	Endpoint string `mapstructure:"endpoint"`
	// ACM RegionId
	RegionId string `mapstructure:"regionId"`
	// ACM AccessKey
	AccessKey string `mapstructure:"accessKey"`
	// ACM SecretKey
	SecretKey string `mapstructure:"secretKey"`
	// ACM OpenKMS
	OpenKMS bool `mapstructure:"openKMS"`
}

type Option struct {
	Hosts       string `mapstructure:"host"`
	Port        uint64 `mapstructure:"port"`
	NamespaceId string `mapstructure:"namespace"`
	GroupName   string `mapstructure:"group"`
	Config      Config `mapstructure:"configserver"`
	Auth        *Auth  `mapstructure:"auth"`
}

type nacosConfigManager struct {
	client config_client.IConfigClient
	option *Option
}

type remoteConfigProvider struct {
	ConfigManager *nacosConfigManager
}

type ViperRemoteProvider struct {
	configType string
	configSet  string
}

type nacosRemoteProvider struct {
	provider      string
	endpoint      string
	path          string
	secretKeyring string
}

func NewNacosConfigManager(option *Option) (*nacosConfigManager, error) {
	var sc []constant.ServerConfig
	hosts := strings.Split(option.Hosts, ";")

	for _, host := range hosts {
		sc = append(sc, constant.ServerConfig{
			ContextPath: "/nacos",
			IpAddr:      host,
			Port:        option.Port,
			Scheme:      "http",
		})
	}

	cc := constant.ClientConfig{
		NamespaceId:         option.NamespaceId,
		TimeoutMs:           viper.GetUint64("nacos.timeout"),
		NotLoadCacheAtStart: true,
		LogDir:              viper.GetString("nacos.logDir"),
		CacheDir:            viper.GetString("nacos.cacheDir"),
		LogRollingConfig: &constant.ClientLogRollingConfig{
			MaxAge: 3,
		},
		LogLevel: "info",
	}

	if option.Auth != nil && option.Auth.Enable {
		cc.Username = option.Auth.User
		cc.Password = option.Auth.Password
		cc.Endpoint = option.Auth.Endpoint
		cc.RegionId = option.Auth.RegionId
		cc.AccessKey = option.Auth.AccessKey
		cc.SecretKey = option.Auth.SecretKey
		cc.OpenKMS = option.Auth.OpenKMS
	}

	configClient, err := clients.NewConfigClient(vo.NacosClientParam{
		ClientConfig:  &cc,
		ServerConfigs: sc,
	})

	if err != nil {
		logger.Error(err.Error())
		return nil, err
	}

	manager := &nacosConfigManager{client: configClient, option: option}

	return manager, err
}

func (ncm *nacosConfigManager) Get(dataId string) ([]byte, error) {
	//get config
	content, err := ncm.client.GetConfig(vo.ConfigParam{
		DataId: ncm.option.Config.DataId,
		Group:  ncm.option.GroupName,
	})
	return []byte(content), err
}

func (ncm *nacosConfigManager) Watch(dataId string, stop chan bool) <-chan *viper.RemoteResponse {
	resp := make(chan *viper.RemoteResponse)

	configParams := vo.ConfigParam{
		DataId: ncm.option.Config.DataId,
		Group:  ncm.option.GroupName,
		OnChange: func(namespace, group, dataId, data string) {
			fmt.Println("config changed group:" + group + ", dataId:" + dataId)
			resp <- &viper.RemoteResponse{
				Value: []byte(data),
				Error: nil,
			}
		},
	}
	err := ncm.client.ListenConfig(configParams)
	if err != nil {
		return nil
	}

	go func() {
		for {
			select {
			case <-stop:
				fmt.Println("stop listening group:" + ncm.option.GroupName + ", dataId:" + dataId)
				_ = ncm.client.CancelListenConfig(configParams)
				return
			}
		}
	}()

	return resp
}

func SetOptions(option *Option) {
	manager, _ := NewNacosConfigManager(option)
	viper.SupportedRemoteProviders = append(viper.SupportedRemoteProviders, "nacos") //[]string{"nacos"}
	viper.RemoteConfig = &remoteConfigProvider{ConfigManager: manager}

}

func (rc *remoteConfigProvider) getConfigManager(rp viper.RemoteProvider) (interface{}, error) {
	if rp.Provider() == "nacos" {
		return rc.ConfigManager, nil
	} else {
		return nil, errors.New("the Nacos configuration manager is not supported")
	}
}

func (rc *remoteConfigProvider) Get(rp viper.RemoteProvider) (io.Reader, error) {
	cm, err := rc.getConfigManager(rp)
	if err != nil {
		return nil, err
	}
	var b []byte
	switch ncm := cm.(type) {
	case viperConfigManager:
		b, err = ncm.Get(rp.Path())
	}
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(b), nil
}

func (rc *remoteConfigProvider) Watch(rp viper.RemoteProvider) (io.Reader, error) {
	return rc.Get(rp)
}

func (rc *remoteConfigProvider) WatchChannel(rp viper.RemoteProvider) (<-chan *viper.RemoteResponse, chan bool) {
	cm, err := rc.getConfigManager(rp)
	if err != nil {
		return nil, nil
	}

	switch ncm := cm.(type) {
	case viperConfigManager:
		quit := make(chan bool)
		viperResponseCh := ncm.Watch("dataId", quit)
		return viperResponseCh, quit
	}

	return nil, nil
}

func DefaultNacosRemoteProvider() *nacosRemoteProvider {
	return &nacosRemoteProvider{provider: "nacos", endpoint: "localhost", path: "", secretKeyring: ""}
}

func (nrp nacosRemoteProvider) Provider() string {
	return nrp.provider
}

func (nrp nacosRemoteProvider) Endpoint() string {
	return nrp.endpoint
}

func (nrp nacosRemoteProvider) Path() string {
	return nrp.path
}

func (nrp nacosRemoteProvider) SecretKeyring() string {
	return nrp.secretKeyring
}

func NewRemoteProvider(configType string) *ViperRemoteProvider {
	return &ViperRemoteProvider{
		configType: configType,
		configSet:  ""}
}

func (provider *ViperRemoteProvider) GetProvider(runtimeViper *viper.Viper) *viper.Viper {
	var option *Option
	err := runtimeViper.Sub(provider.configSet).Unmarshal(&option)
	if err != nil {
		panic(err)
		return nil
	}
	SetOptions(option)
	remoteViper := viper.New()
	err = remoteViper.AddRemoteProvider("nacos", "localhost", "")
	if provider.configType == "" {
		provider.configType = "yaml"
	}

	remoteViper.SetConfigType(provider.configType)
	err = remoteViper.ReadRemoteConfig()
	if err == nil {
		//err = remote_viper.WatchRemoteConfigOnChannel()
		if err == nil {
			fmt.Println("used remote viper")
			return remoteViper
		}
	} else {
		panic(err)
	}
	return runtimeViper
}

func (provider *ViperRemoteProvider) WatchRemoteConfigOnChannel(remoteViper *viper.Viper) <-chan bool {
	updater := make(chan bool)

	respChan, _ := viper.RemoteConfig.WatchChannel(DefaultNacosRemoteProvider())
	go func(rc <-chan *viper.RemoteResponse) {
		for {
			b := <-rc
			reader := bytes.NewReader(b.Value)
			_ = remoteViper.ReadConfig(reader)
			// configuration on changed
			updater <- true
		}
	}(respChan)

	return updater
}

func NacosInit() {

	SetOptions(&Option{
		Hosts:       viper.GetString("nacos.host"),
		Port:        viper.GetUint64("nacos.port"),
		NamespaceId: viper.GetString("nacos.nameSpaceId"),
		GroupName:   viper.GetString("nacos.group"),
		Config:      Config{DataId: viper.GetString("nacos.dataId")},
		Auth: &Auth{
			Enable:    true,
			User:      viper.GetString("nacos.user"),
			Password:  viper.GetString("nacos.password"),
			SecretKey: "",
		},
	})

	err := viper.AddRemoteProvider("nacos", viper.GetString("nacos.host"), "")
	err = viper.ReadRemoteConfig()
	err = viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("Fatal error read remote config file: %s \n", err))
	}
}
