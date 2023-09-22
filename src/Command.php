<?php

namespace Wind\Memcache;

use Amp\DeferredFuture;
use Amp\Future;
use Throwable;
use Wind\Socket\SimpleTextCommand;

/*
Redis binary protocol
https://github.com/memcached/memcached/wiki/BinaryProtocolRevamped

    Request header

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| Magic         | Opcode        | Key length                    |
       +---------------+---------------+---------------+---------------+
      4| Extras length | Data type     | vbucket id                    |
       +---------------+---------------+---------------+---------------+
      8| Total body length                                             |
       +---------------+---------------+---------------+---------------+
     12| Opaque                                                        |
       +---------------+---------------+---------------+---------------+
     16| CAS                                                           |
       |                                                               |
       +---------------+---------------+---------------+---------------+
       Total 24 bytes

    Request header

     Byte/     0       |       1       |       2       |       3       |
        /              |               |               |               |
       |0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|0 1 2 3 4 5 6 7|
       +---------------+---------------+---------------+---------------+
      0| Magic         | Opcode        | Key Length                    |
       +---------------+---------------+---------------+---------------+
      4| Extras length | Data type     | Status                        |
       +---------------+---------------+---------------+---------------+
      8| Total body length                                             |
       +---------------+---------------+---------------+---------------+
     12| Opaque                                                        |
       +---------------+---------------+---------------+---------------+
     16| CAS                                                           |
       |                                                               |
       +---------------+---------------+---------------+---------------+
       Total 24 bytes
*/

/**
 * Memcache binary protocol command
 */
class Command implements SimpleTextCommand
{

    private DeferredFuture $deferred;

    public function __construct(private int $opcode, private ?string $key=null, private ?string $value=null, private ?string $extras=null)
    {
        $this->deferred = new DeferredFuture;
    }

    public function getFuture(): Future
    {
        return $this->deferred->getFuture();
    }

    public function encode(): string
    {
        $keyLength = $this->key !== null ? strlen($this->key) : 0;
        $extraLength = $this->extras === null ? 0 : strlen($this->extras);
        $valueLength = $this->value === null ? 0 : strlen($this->value);

        $cmd = pack(
            'CCnCCnNNJ',
            0x80, $this->opcode, $keyLength,
            $extraLength, 0, 0,
            $keyLength + $extraLength + $valueLength,
            0,
            0
        );

        $this->extras !== null && $cmd .= $this->extras;
        $this->key !== null && $cmd .= $this->key;
        $this->value !== null && $cmd .= $this->value;

        return $cmd;
    }

    public function resolve(string|Throwable $buffer)
    {
        if ($buffer instanceof Throwable) {
            $this->deferred->error($buffer);
        } else {
            $header = unpack('Cmagic/Copcode/nkeylength/Cextraslength/Cdatatype/nstatus/Ntotalbodylength/Nopaque/Jcas', $buffer);

            // $extras = $header['extraslength'] > 0 ? substr($buffer, 24, $header['extraslength']) : '';
            // $key = $header['keylength'] > 0 ? substr($buffer, 24 + $header['extraslength'], $header['keylength']) : '';
            $body = $header['totalbodylength'] > 0 ? substr($buffer, 24 + $header['extraslength'] + $header['keylength'], $header['totalbodylength'] - $header['extraslength'] - $header['keylength']) : '';

            switch ($header['status']) {
                case 0x00;
                    $this->deferred->complete($body);
                    break;

                case 0x01:
                    $this->deferred->complete(false);
                    break;

                default:
                    $this->deferred->error(new MemcacheException($body));
            }
        }
    }

    public static function bytes(string $buffer): int
    {
        $unpack = unpack('Copcode/nkeylength', substr($buffer, 1, 3));
        $unpack += unpack('Ntotalbodylength', substr($buffer, 8, 4));

        if ($unpack['opcode'] == 0x10) {
            print_r($unpack);
        }

        return strlen($buffer) - 24 - $unpack['totalbodylength'];
    }

}
