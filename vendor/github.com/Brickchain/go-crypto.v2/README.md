# crypto-lib

## Usage
```
import (
    "fmt"
    "gopkg.in/square/go-jose.v1"
    crypto "github.com/Brickchain/go-crypto.v1"
)

func encrypt() {

    // generate a new private key
    key, err := crypto.NewKey()
    if err != nil {
        panic(err)
    }
    
    // get the recipients public key from somewhere...
    pk, err := crypto.UnmarshalKey(SomeJsonBytes())
    if err != nil {
        panic(err)
    }
    
    // create a new "encrypter", it's like a manager for the encryption opteration
    enc, err := crypto.NewEncrypter()
    if err != nil {
        panic(err)
    }
    
    // add the recipient
    enc.AddRecipient(pk)
    
    // encrypt some text
    msg, err := enc.Encrypt([]byte("some text"))
    if err != nil {
        panic(err)
    }
    
    // print the JsonWebEncryption object serialized to JSON
    fmt.Println(msg.FullSerialize())
}

func decrypt(mykey *jose.JsonWebKey, message *jose.JsonWebEncryption) {
    _, _, m, err := message.DecryptMulti(mykey)
    if err != nil {
        panic(err)
    }
    
    fmt.Println(string(m))
}

func sign(mykey *jose.JsonWebKey) {
    signer, err := crypto.NewSigner(mykey)
    if err != nil {
        panic(err)
    }
    
    signature, err := signer.Sign([]byte("some test string"))
    if err != nil {
        panic(err)
    }

    fmt.Println(signature.FullSerialize())
}

func verifySign(publicKey *jose.JsonWebKey, message *jose.JsonWebSignature) {
    _, _, payload, err := message.VerifyMulti(publicKey)
    if err != nil {
        panic(err)
    }
    fmt.Println(string(payload))
}
```