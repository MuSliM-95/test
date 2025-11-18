import argparse
from cryptography.fernet import Fernet


def generate_key():
    """Generate a new Fernet encryption key"""
    key = Fernet.generate_key()
    return key.decode()


def validate_key(key_str: str) -> bool:
    try:
        Fernet(key_str.encode())
        return True
    except Exception as e:
        print(f"Invalid key: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Manage Avito API encryption keys",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python encryption_key_manager.py --generate
  python encryption_key_manager.py --validate "your-key-here"
  python encryption_key_manager.py --test "your-key-here"
        """
    )
    
    parser.add_argument(
        "--generate",
        action="store_true",
        help="Generate a new encryption key"
    )
    
    parser.add_argument(
        "--validate",
        type=str,
        metavar="KEY",
        help="Validate an encryption key"
    )
    
    parser.add_argument(
        "--test",
        type=str,
        metavar="KEY",
        help="Test encryption/decryption with a key"
    )
    
    args = parser.parse_args()
    
    if not any([args.generate, args.validate, args.test]):
        parser.print_help()
        return
    
    if args.generate:
        print("Generating new encryption key...\n")
        key = generate_key()
        print("New encryption key:")
        print("═" * 70)
        print(f"{key}")
        print("═" * 70)
        print("\nCopy this to your .env or environment variable:")
        print(f"export AVITO_ENCRYPTION_KEY=\"{key}\"")
        print()
    
    if args.validate:
        print(f"Validating key: {args.validate[:50]}...\n")
        if validate_key(args.validate):
            print("Key is valid!")
        print()


if __name__ == "__main__":
    main()
