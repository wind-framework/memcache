<?php

namespace Wind\Memcache;

use Amp\Socket\SocketConnector;
use Amp\Socket\Socket;
use Wind\Socket\SimpleTextClient;

/**
 * Wind Framework Memcache Client
 */
class Memcache extends SimpleTextClient
{

    public function __construct(private string $host='127.0.0.1', private int $port=11211)
    {
        parent::__construct();
        $this->autoReconnect = true;
        $this->connect();
    }

    protected function createSocket(SocketConnector $connector): Socket
    {
        return $connector->connect("tcp://{$this->host}:{$this->port}");
    }

    protected function authenticate() {}

    protected function cleanResources() {}

    protected function bytes(string $buffer): int
    {
        return Command::bytes($buffer);
    }

    public function get($key)
    {
        return $this->execute(new Command(0x00, $key));
    }

    public function set($key, $value, $expiry=0)
    {
        return $this->store(0x01, $key, $value, $expiry);
    }

    public function add($key, $value, $expiry=0)
    {
        return $this->store(0x01, $key, $value, $expiry);
    }

    public function replace($key, $value, $expiry=0)
    {
        return $this->store(0x03, $key, $value, $expiry);
    }

    protected function store($opcode, $key, $value, $expiry)
    {
        $command = new Command($opcode, $key, $value, pack('NN', 0xdeadbeef, $expiry));
        return $this->execute($command);
    }

    public function delete($key)
    {
        return $this->execute(new Command(0x04, $key));
    }

    public function increment($key, $amount=1, $expiry=0)
    {
        return $this->number(0x05, $key, $amount, $expiry);
    }

    public function decrement($key, $amount=1, $expiry=0)
    {
        return $this->number(0x06, $key, $amount, $expiry);
    }

    protected function number($opcode, $key, $amount, $expiry)
    {
        $command = new Command($opcode, $key, null, pack('JJN', $amount, 1, $expiry));
        return $this->execute($command);
    }

    public function flush($expiration=0)
    {
        $extras = $expiration != 0 ? pack('N', $expiration) : null;
        return $this->execute(new Command(0x08, extras: $extras));
    }

    public function noop()
    {
        return $this->execute(new Command(0x0a));
    }

    public function version()
    {
        return $this->execute(new Command(0x0b));
    }

    public function append($key, $value)
    {
        return $this->execute(new Command(0x0e, $key, $value));
    }

    public function prepend($key, $value)
    {
        return $this->execute(new Command(0x0f, $key, $value));
    }

    public function stats($key=null)
    {
        return $this->execute(new Command(0x10, $key));
    }

}
