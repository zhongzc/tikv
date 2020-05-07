#![feature(proc_macro_diagnostic)]
#![recursion_limit = "256"]

extern crate proc_macro;

use proc_macro::TokenStream;

#[proc_macro_attribute]
pub fn future01_fn_root(args: TokenStream, item: TokenStream) -> TokenStream {
    let input = syn::parse_macro_input!(item as syn::ItemFn);
    let tag = syn::parse_macro_input!(args as syn::Expr);

    let syn::ItemFn {
        attrs,
        vis,
        block,
        sig,
        ..
    } = input;

    let syn::Signature {
        output: return_type,
        inputs: params,
        unsafety,
        asyncness,
        constness,
        abi,
        ident,
        generics:
            syn::Generics {
                params: gen_params,
                where_clause,
                ..
            },
        ..
    } = sig;

    quote::quote!(
        #(#attrs) *
        #vis #constness #unsafety #asyncness #abi fn #ident<#gen_params>(#params) #return_type
        #where_clause
        {
            let (__span_tx, __span_rx) = minitrace::Collector::new(minitrace::DEFAULT_COLLECTOR);
            let __span = minitrace::new_span_root(__span_tx, #tag);
            let __g = __span.enter();

            {
                #block
            }.inspect(move |_| {
                let __spans = __span_rx.collect_all();

                let __bytes = trace_pb::serialize(__spans.into_iter());
                // avoid dead-code elimination
                let __bytes = unsafe {
                    let ret = std::ptr::read_volatile(&__bytes);
                    std::mem::forget(__bytes);
                    ret
                };

                // for __s in __spans {
                //     println!("{:?} {:?} {:?} {:?} {:?}", __s.id, __s.parent, __s.elapsed_start, __s.elapsed_end, __s.tag);
                // }
                // println!();
            })
        }
    )
    .into()
}