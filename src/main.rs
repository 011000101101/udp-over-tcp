use std::net::Ipv4Addr;

use eyre::WrapErr;
use lexopt::prelude::*;

use udp_over_tcp::{port_or_addr, UdpToTcp};

#[tokio::main(flavor = "current_thread")]
async fn main() -> eyre::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::filter::EnvFilter::from_default_env())
        .init();

    let mut listen = false;
    let mut tcp_addr = None;
    let mut udp_bind = None;
    let mut udp_sendto = None;

    let mut parser = lexopt::Parser::from_env();
    while let Some(arg) = parser.next().wrap_err("parse arguments")? {
        match arg {
            Long("tcp-listen") | Short('l') if tcp_addr.is_none() => {
                listen = true;
                tcp_addr = Some(
                    parser
                        .value()
                        .wrap_err("value missing")
                        .and_then(|v| port_or_addr(v, Ipv4Addr::UNSPECIFIED))
                        .wrap_err("--tcp-listen")?,
                );
            }
            Long("tcp-connect") | Short('t') if tcp_addr.is_none() => {
                listen = false;
                tcp_addr = Some(
                    parser
                        .value()
                        .wrap_err("value missing")
                        .and_then(|v| port_or_addr(v, Ipv4Addr::LOCALHOST))
                        .wrap_err("--tcp-connect")?,
                );
            }
            Long("udp-bind") | Short('u') if udp_bind.is_none() => {
                udp_bind = Some(
                    parser
                        .value()
                        .wrap_err("value missing")
                        .and_then(|v| port_or_addr(v, Ipv4Addr::UNSPECIFIED))
                        .wrap_err("--udp-bind")?,
                );
            }
            Long("udp-sendto") | Short('p') if udp_sendto.is_none() => {
                udp_sendto = Some(
                    parser
                        .value()
                        .wrap_err("value missing")
                        .and_then(|v| port_or_addr(v, Ipv4Addr::LOCALHOST))
                        .wrap_err("--udp-sendto")?,
                );
            }
            Short('h') | Long("help") => {
                usage(0);
            }
            _ => return Err(arg.unexpected()).wrap_err("unexpected argument"),
        }
    }

    let Some(tcp_addr) = tcp_addr else {
        usage(1);
    };
    let Some(udp_bind) = udp_bind else {
        eyre::bail!("no udp port given");
    };
    let Some(udp_sendto) = udp_sendto else {
        eyre::bail!("no udp forward destination given");
    };

    let mut udp_to_tcp = UdpToTcp::new(listen, tcp_addr, udp_bind, udp_sendto);

    udp_to_tcp.run(udp_bind).await
}

fn usage(exit_with: i32) -> ! {
    let bin = std::env::args()
        .next()
        .unwrap_or_else(|| String::from(env!("CARGO_BIN_NAME")));

    eprintln!(
        "{}",
        concat!(env!("CARGO_BIN_NAME"), " ", env!("CARGO_PKG_VERSION"))
    );
    eprintln!("https://github.com/jonhoo/udp-over-tcp");
    eprintln!();
    eprintln!("You have a UDP application running on host X on port A.");
    eprintln!("You want it to talk to a UDP application running on host Y on port B.");
    eprintln!("And you also want to allow the application on Y to talk to A on X.");
    eprintln!("Great, do as follows:");
    eprintln!();
    eprintln!("On either host (here X), first create a TCP tunnel to the other host:");
    eprintln!();
    eprintln!("    ssh -L 7878:127.0.0.1:7878 $Y");
    eprintln!();
    eprintln!("Next, run udp-over-tcp on both hosts, one with `--tcp-listen` and one with `--tcp-connect`.");
    eprintln!("The `--tcp-listen` should be used on the host that the forwarding allows connecting _to_ (here Y).");
    eprintln!("You can run them in either order, but best practice is to listen first:");
    eprintln!();
    eprintln!("    Y $ {bin} --tcp-listen  7878 --udp-bind $A --udp-sendto $B");
    eprintln!("    X $ {bin} --tcp-connect 7878 --udp-bind $B --udp-sendto $A");
    eprintln!();
    eprintln!("On Y, this will listen on UDP port $A, forward those over TCP to X, and then deliver them to UDP port $A there.");
    eprintln!("On X, this will listen on UDP port $B, forward those over TCP to Y, and then deliver them to UDP port $B there.");
    eprintln!();
    eprintln!("Now configure the application on X to send to 127.0.0.1:$B");
    eprintln!("and configure the application on Y to send to 127.0.0.1:$A.");
    eprintln!("In other words, same port, local IP address.");
    eprintln!();
    eprintln!("Each argument takes a port number (as above) or addr:port to specify the address.");
    eprintln!("(address defaults to 0.0.0.0 for listen/bind and 127.0.0.1 for connect/sendto)");
    std::process::exit(exit_with);
}
