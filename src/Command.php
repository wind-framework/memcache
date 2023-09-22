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

    private const HEADER_LENGTH = 24;

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
        $extrasLength = $this->extras === null ? 0 : strlen($this->extras);
        $valueLength = $this->value === null ? 0 : strlen($this->value);

        $cmd = pack(
            'CCnCCnNNJ',
            0x80, $this->opcode, $keyLength,
            $extrasLength, 0, 0, //data type, vbucket id
            $keyLength + $extrasLength + $valueLength,
            0, //opaque
            0 //cas
        );

        $this->extras !== null && $cmd .= $this->extras;
        $this->key !== null && $cmd .= $this->key;
        $this->value !== null && $cmd .= $this->value;

        return $cmd;
    }

    public function decode($buffer)
    {
        $header = unpack('Cmagic/Copcode/nkeylength/Cextraslength/Cdatatype/nstatus/Ntotalbodylength/Nopaque/Jcas', $buffer);
        $prependLength = $header['extraslength'] + $header['keylength'];
        return $header + [
            'extras' => $header['extraslength'] > 0 ? substr($buffer, self::HEADER_LENGTH, $header['extraslength']) : '',
            'key' => $header['keylength'] > 0 ? substr($buffer, self::HEADER_LENGTH + $header['extraslength'], $header['keylength']) : null,
            'value' => $header['totalbodylength'] > $prependLength ? substr($buffer, self::HEADER_LENGTH + $prependLength, $header['totalbodylength'] - $prependLength) : ''
        ];
    }

    public function resolve(string|Throwable $buffer)
    {
        if ($buffer instanceof Throwable) {
            $this->deferred->error($buffer);
        } else {
            $data = $this->decode($buffer);

            switch ($data['status']) {
                case 0x00;
                    if ($data['opcode'] == 0x10) {
                        $values = [];
                        do {
                            $values[$data['key']] = $data['value'];
                            $buffer = substr($buffer, self::HEADER_LENGTH + $data['totalbodylength']);
                            $data = $this->decode($buffer);
                        } while ($data['key'] !== null);
                        $this->deferred->complete($values);
                    } else {
                        $this->deferred->complete($data['value']);
                    }
                    break;

                case 0x01:
                    $this->deferred->complete(false);
                    break;

                default:
                    $this->deferred->error(new MemcacheException($data['value']));
            }
        }
    }

    public static function bytes(string $buffer): int
    {
        $unpack = unpack('Copcode', substr($buffer, 1, 1));
        $unpack += unpack('Ntotalbodylength', substr($buffer, 8, 4));

        if ($unpack['opcode'] == 0x10) {
            return 0;
        }

        return strlen($buffer) - self::HEADER_LENGTH - $unpack['totalbodylength'];
    }

}
