use IO::Async::Stream;
use IO::Async::Timer::Periodic;

use IO::Async::Loop;
use Scalar::Util qw(
  blessed
);
use Try::Tiny;

use Kafka qw(
  $BITS64
  $DEFAULT_MAX_BYTES
  $DEFAULT_MAX_NUMBER_OF_OFFSETS
  $RECEIVE_EARLIEST_OFFSET
);
use Kafka::Connection;
use Kafka::Producer;
use Kafka::Consumer;
use Log::Log4perl qw(:easy);
use Data::UUID;

Log::Log4perl->easy_init($DEBUG);

my ( $connection, $producer, $consumer, $loop, $ug );

$loop = IO::Async::Loop->new;
$ug   = Data::UUID->new;

try {
    DEBUG 'Connecting to broker ', $ENV{'BROKER_SERVERS'};
    $connection = Kafka::Connection->new( host => $ENV{'BROKER_SERVERS'} );
    $producer   = Kafka::Producer->new( Connection => $connection );
    $consumer   = Kafka::Consumer->new( Connection => $connection );

}
catch {
    my $error = $_;
    if ( blessed($error) && $error->isa('Kafka::Exception') ) {
        warn 'Error: (', $error->code, ') ', $error->message, "\n";
        exit;
    }
    else {
        die $error;
    }
};

# consumer
my $consumer_offset = 0;
$loop->add(
    IO::Async::Timer::Periodic->new(
        interval => 1,
        on_tick  => sub {
            try {
                my $messages = $consumer->fetch( 'uuid', 0, $consumer_offset,
                    $DEFAULT_MAX_BYTES );
                if ($messages) {
                    foreach my $message (@$messages) {
                        if ( $message->valid ) {
                            DEBUG '[consumer] payload    : ', $message->payload;
                            DEBUG '[consumer] key        : ', $message->key;
                            DEBUG '[consumer] offset     : ', $message->offset;
                            DEBUG '[consumer] next_offset: ',
                              $message->next_offset;
                        }
                        else {
                            ERROR '[consumer] error      : ', $message->error;
                        }
                        $consumer_offset = $message->next_offset;
                    }
                }
                catch {
                    my $error = $_;
                    ERROR 'Error: (', $error->code, ') ', $error->message, "\n";
                }
            }

        },
    )->start
);

# publisher
$loop->add(
    IO::Async::Timer::Periodic->new(
        interval => 2,
        on_tick  => sub {
            try {
                my $message = $ug->to_string( $ug->create() );
                DEBUG "[publisher] sending ", $message;
                $producer->send(
                    'uuid',    # topic
                    0,         # partition
                    $message
                );
            }
            catch {
                my $error = $_;
                ERROR '[publisher] Error: (', $error->code, ') ',
                  $error->message, "\n";
            }
        },
    )->start
);

$loop->run;
