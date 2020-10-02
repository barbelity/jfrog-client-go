package services

import (
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/jfrog/gofrog/parallel"
	rthttpclient "github.com/jfrog/jfrog-client-go/artifactory/httpclient"
	"github.com/jfrog/jfrog-client-go/artifactory/services/utils"
	"github.com/jfrog/jfrog-client-go/auth"
	clientutils "github.com/jfrog/jfrog-client-go/utils"
	"github.com/jfrog/jfrog-client-go/utils/errorutils"
	"github.com/jfrog/jfrog-client-go/utils/io/content"
	"github.com/jfrog/jfrog-client-go/utils/log"
)

type PropsService struct {
	client     *rthttpclient.ArtifactoryHttpClient
	ArtDetails auth.ServiceDetails
	Threads    int
}

func NewPropsService(client *rthttpclient.ArtifactoryHttpClient) *PropsService {
	return &PropsService{client: client}
}

func (ps *PropsService) GetArtifactoryDetails() auth.ServiceDetails {
	return ps.ArtDetails
}

func (ps *PropsService) SetArtifactoryDetails(rt auth.ServiceDetails) {
	ps.ArtDetails = rt
}

func (ps *PropsService) IsDryRun() bool {
	return false
}

func (ps *PropsService) GetThreads() int {
	return ps.Threads
}

func (ps *PropsService) GetJfrogHttpClient() (*rthttpclient.ArtifactoryHttpClient, error) {
	return ps.client, nil
}

func (ps *PropsService) SetDeleteProps(propsParams PropsParams, isDelete bool) (successCount, failedCount int, err error) {
	resultReader, err := ps.performSearch(propsParams)
	if err != nil || resultReader == nil {
		return
	}
	defer resultReader.Close()
	propsReaderParams := PropsReaderParams{Reader: resultReader, Properties: propsParams.Properties}

	if isDelete {
		return ps.DeletePropsWithReader(propsReaderParams)
	}
	return ps.SetPropsWithReader(propsReaderParams)
}

func (ps *PropsService) SetPropsWithReader(propsParams PropsReaderParams) (successCount, failedCount int, err error) {
	log.Info("Setting properties...")
	successCount, failedCount, err = ps.performRequest(propsParams.GetReader(), propsParams.GetProperties(), false)
	if err == nil {
		log.Info("Done setting properties.")
	}
	return
}

func (ps *PropsService) DeletePropsWithReader(propsParams PropsReaderParams) (successCount, failedCount int, err error) {
	log.Info("Deleting properties...")
	successCount, failedCount, err = ps.performRequest(propsParams.GetReader(), propsParams.GetProperties(), true)
	if err == nil {
		log.Info("Done deleting properties.")
	}
	return
}

type PropsParams struct {
	*utils.ArtifactoryCommonParams
	Properties string
}

func (pp *PropsParams) GetProperties() string {
	return pp.Properties
}

func (pp *PropsParams) GetFile() *utils.ArtifactoryCommonParams {
	return pp.ArtifactoryCommonParams
}

func (pp *PropsParams) SetIncludeDir(isIncludeDir bool) {
	pp.GetFile().IncludeDirs = isIncludeDir
}

type PropsReaderParams struct {
	Reader     *content.ContentReader
	Properties string
}

func (prp *PropsReaderParams) GetReader() *content.ContentReader {
	return prp.Reader
}

func (prp *PropsReaderParams) GetProperties() string {
	return prp.Properties
}

func (ps *PropsService) performRequest(reader *content.ContentReader, properties string, isDelete bool) (successCount, failedCount int, err error) {
	var encodedParam string
	if !isDelete {
		var props *utils.Properties
		props, err = utils.ParseProperties(properties, utils.JoinCommas)
		if err != nil {
			return
		}
		encodedParam = props.ToEncodedString()
	} else {
		propList := strings.Split(properties, ",")
		for _, prop := range propList {
			encodedParam += url.QueryEscape(prop) + ","
		}
		// Remove trailing comma
		if strings.HasSuffix(encodedParam, ",") {
			encodedParam = encodedParam[:len(encodedParam)-1]
		}
	}

	successCounters := make([]int, ps.GetThreads())
	producerConsumer := parallel.NewBounedRunner(ps.GetThreads(), false)
	errorsQueue := clientutils.NewErrorsQueue(1)
	go func() {
		for resultItem := new(utils.ResultItem); reader.NextRecord(resultItem) == nil; resultItem = new(utils.ResultItem) {
			relativePath := resultItem.GetItemRelativePath()
			setPropsTask := func(threadId int) error {
				var err error
				logMsgPrefix := clientutils.GetLogMsgPrefix(threadId, ps.IsDryRun())

				restApi := path.Join("api", "storage", relativePath)
				setPropertiesUrl, err := utils.BuildArtifactoryUrl(ps.GetArtifactoryDetails().GetUrl(), restApi, make(map[string]string))
				if err != nil {
					return err
				}
				setPropertiesUrl += "?properties=" + encodedParam

				var resp *http.Response
				if isDelete {
					resp, _, err = ps.sendDeleteRequest(logMsgPrefix, relativePath, setPropertiesUrl)
				} else {
					resp, _, err = ps.sendPutRequest(logMsgPrefix, relativePath, setPropertiesUrl)
				}

				if err != nil {
					return err
				}
				if err = errorutils.CheckResponseStatus(resp, http.StatusNoContent); err != nil {
					return errorutils.CheckError(err)
				}
				successCounters[threadId]++
				return nil
			}

			producerConsumer.AddTaskWithError(setPropsTask, errorsQueue.AddError)
		}
		defer producerConsumer.Done()
		if err := reader.GetError(); err != nil {
			errorsQueue.AddError(err)
		}
		reader.Reset()
	}()

	producerConsumer.Run()
	for _, v := range successCounters {
		successCount += v
	}
	totalReaderLength, err := reader.Length()
	if err != nil {
		return
	}
	failedCount = totalReaderLength - successCount
	err = errorsQueue.GetError()
	return
}

func (ps *PropsService) sendDeleteRequest(logMsgPrefix, relativePath, setPropertiesUrl string) (resp *http.Response, body []byte, err error) {
	log.Info(logMsgPrefix+"Deleting properties on:", relativePath)
	log.Debug(logMsgPrefix+"Sending delete properties request:", setPropertiesUrl)
	httpClientsDetails := ps.GetArtifactoryDetails().CreateHttpClientDetails()
	resp, body, err = ps.client.SendDelete(setPropertiesUrl, nil, &httpClientsDetails)
	return
}

func (ps *PropsService) sendPutRequest(logMsgPrefix, relativePath, setPropertiesUrl string) (resp *http.Response, body []byte, err error) {
	log.Info(logMsgPrefix+"Setting properties on:", relativePath)
	log.Debug(logMsgPrefix+"Sending set properties request:", setPropertiesUrl)
	httpClientsDetails := ps.GetArtifactoryDetails().CreateHttpClientDetails()
	resp, body, err = ps.client.SendPut(setPropertiesUrl, nil, &httpClientsDetails)
	return
}

func (ps *PropsService) performSearch(propsParams PropsParams) (*content.ContentReader, error) {
	log.Info("Searching items...")
	switch propsParams.GetSpecType() {
	case utils.BUILD:
		return utils.SearchBySpecWithBuild(propsParams.GetFile(), ps)
	case utils.AQL:
		return utils.SearchBySpecWithAql(propsParams.GetFile(), ps, utils.NONE)
	case utils.WILDCARD:
		propsParams.SetIncludeDir(true)
		return utils.SearchBySpecWithPattern(propsParams.GetFile(), ps, utils.NONE)
	}
	return nil, nil
}

func NewPropsParams() PropsParams {
	return PropsParams{}
}

func NewPropsReaderParams() PropsReaderParams {
	return PropsReaderParams{}
}
