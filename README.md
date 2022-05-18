# publisher

Watermill publisher that is setup to use [watermill-googlecloud](https://github.com/ThreeDotsLabs/watermill-googlecloud) to publish messages, and which is preconfigured for distributed Opentelemetry tracing. For this we use both the official [watermill-opentelemetry](https://github.com/voi-oss/watermill-opentelemetry) project and our custom complement [dentech-floss/watermill-opentelemetry-go-extra](https://github.com/dentech-floss/watermill-opentelemetry-go-extra) so a span is created when a message is published and which then is propagated to the subscriber(s) for extraction.

Also, this lib take care of the creation of Watermill messages carrying protobuf payload (marshalling + making sure that the context is set on the message to enable the above mentioned tracing) so please use the provided "NewMessage" func as shown below in the example :)

## Install

```
go get github.com/dentech-floss/publisher@v0.1.1
```

## Usage

Create the publisher:

```go
package example

import (
    "github.com/dentech-floss/logging/pkg/logging"
    "github.com/dentech-floss/publisher/pkg/publisher"
)

func main() {
    logger := logging.NewLogger(&logging.LoggerConfig{})
    defer logger.Sync()

    publisher := publisher.NewPublisher(
        logger.Logger.Logger, // the *zap.Logger is wrapped like a matryoshka doll :)
        &publisher.PublisherConfig{
            OnGCP: true,
            ProjectId: "mysuperduperproject",  // only applicable if OnGCP is true
        },
    )
    defer publisher.Close()

    appointmentServiceV1 := service.NewAppointmentServiceV1(repo, publisher, logger) // inject it
}
```

Example of how to use the publisher to publish a protobuf domain event on a PubSub topic:

```go
package example

import (
    "context"

    "github.com/dentech-floss/logging/pkg/logging"
    "github.com/dentech-floss/publisher/pkg/publisher"

    appointment_service_v1 "go.buf.build/dentechse/go-grpc-gateway-openapiv2/dentechse/service-definitions/api/appointment/v1"
)

const (
    TOPIC_APPOINTMENT_CLAIMED  = "appointment.claimed"
)

...

func (s *AppointmentServiceV1) ClaimAppointment(
    ctx context.Context,
    request *appointment_service_v1.ClaimAppointmentRequest,
) (*appointment_service_v1.ClaimAppointmentResponse, error) {

    // Ensure trace information + the request is part of the log entries
    logWithContext := s.log.WithContext(ctx, logging.ProtoField("request", request))

    claimed, err := s.repo.ClaimAppointment(...)

    if claimed {
        s.publishAppointmentClaimedEvent(ctx, logWithContext, appointment)
    } else {
        logWithContext.Warn("Appointment was not claimed...")
    }

    ...
}

func (s *AppointmentServiceV1) publishAppointmentClaimedEvent(
    ctx context.Context,
    logWithContext *logging.LoggerWithContext,
    appointment *model.Appointment,
) {
    event := &appointment_service_v1.AppointmentEvent{
        Event: &appointment_service_v1.AppointmentEvent_AppointmentClaimed{
            AppointmentClaimed: &appointment_service_v1.AppointmentClaimedEvent{
                Appointment: s.appointmentToDTO(appointment),
                ClaimedAt:   timestamppb.New(s.timeProvider.Now()),
            },
        },
    }
    s.publishAsync(ctx, logWithContext, TOPIC_APPOINTMENT_CLAIMED, event)
}

func (s *AppointmentServiceV1) publishAsync(
    ctx context.Context,
    logWithContext *logging.LoggerWithContext,
    topic string,
    event *appointment_service_v1.AppointmentEvent,
) {
    go func() {
        msg, err := s.publisher.NewMessage(ctx, event) // do use this method!
        if err != nil {
            logWithContext.Error(
                "Failed to create message",
                logging.StringField("topic", topic),
                logging.ProtoField("payload", event),
                logging.ErrorField(err),
            )
        } else {
            if err := s.publisher.Publish(topic, msg); err != nil {
                logWithContext.Error(
                    "Failed to publish message",
                    logging.StringField("topic", topic),
                    logging.ProtoField("payload", event),
                    logging.ErrorField(err),
                )
            }
        }
    }()
}
```

For testing purposes, the lib comes with a "fake publisher" that can be used when writing tests to verify that a message was/was not published given a certain condition. And if a message was published, then it makes it possible to write assertions on how the actual message looked like:

```go
package example

import (
    "testing"

    "github.com/dentech-floss/datetime/pkg/datetime"
    "github.com/dentech-floss/publisher/pkg/publisher"

    appointment_service_v1 "go.buf.build/dentechse/go-grpc-gateway-openapiv2/dentechse/service-definitions/api/appointment/v1"

    "google.golang.org/protobuf/proto"
    "google.golang.org/protobuf/types/known/timestamppb"

    "github.com/stretchr/testify/require"
)

func Test_ClaimAppointment(t *testing.T) {

    require := require.New(t)

    fp := publisher.NewFakePublisher() // Let's us get hold of published messages
    fakePublisher := fp.(*publisher.FakePublisher)
    publisher := &publisher.Publisher{fp}
    appointmentServiceV1 := service.NewAppointmentServiceV1(publisher) // inject it

    fakePublisher.ClearPublished() // clear any existing messages...

    appointmentServiceV1.ClaimAppointment(...)

    // Verify that a domain event was published

    require.Equal(1, len(fakePublisher.GetPublished()))
    publishedEntry := fakePublisher.GetPublished()[0]
    require.Equal(TOPIC_APPOINTMENT_CLAIMED, publishedEntry.Topic)
    require.Equal(1, len(publishedEntry.Messages))
    publishedMessage := publishedEntry.Messages[0]
    publishedEvent := &appointment_service_v1.AppointmentEvent{}
    err := proto.Unmarshal(publishedMessage.Payload, publishedEvent)
    if err != nil {
        t.Fatal(err)
    }
    switch e := publishedEvent.Event.(type) {
    case *appointment_service_v1.AppointmentEvent_AppointmentClaimed:
        claimedEvent := e.AppointmentClaimed
        require.NotNil(claimedEvent.Appointment)
        appointmentDTO := claimedEvent.Appointment
        require.Equal(util.Int32ToString(appointment.ID), appointmentDTO.Id)
        require.Equal(util.Int32ToString(appointment.CompanyID), appointmentDTO.ClinicId)
        require.Equal(util.Int32ToString(appointment.TreatmentID), appointmentDTO.TreatmentId)
        require.NotNil(appointmentDTO.BookingId)
        require.Equal(util.Int32ToString(bookingId), appointmentDTO.BookingId.Value)
        require.Equal(datetime.TimeToISO8601DateTimeString(appointment.StartTime), appointmentDTO.StartTime)
        require.WithinDuration(timestamppb.New(now).AsTime(), claimedEvent.ClaimedAt.AsTime(), 0)
    default:
        require.FailNow("Unexpected event type")
    }
}

```