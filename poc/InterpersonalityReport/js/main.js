$(".navbar-toggler").click(function(){
	$(".main").toggleClass("nav_fix");
	$(".mobile_nav").toggleClass("mob_fix");
});

$(".friend_section").click(function(){
	$(".main").removeClass("nav_fix");
	$(".mobile_nav").removeClass("mob_fix");
});

(function($) {
	$(document).ready( function() {
		$('[data-toggle="popover"]').popover();

		$('button.toggle-menu').on('click', function(e) {
			$('#mobile-menu').toggleClass('show');
		});

		$('#mobile-menu').on('click', function(e) {
			if( e.target !== $('#mobile-menu div.inner') && $(e.target).parents('div.inner').length == 0 ) {
				$('#mobile-menu').removeClass('show');
			}
		});

		$('.popup-video a').magnificPopup({
			type: 'iframe',
			mainClass: 'mfp-fade',
			removalDelay: 160,
			preloader: false,
			disableOn: 300,
			fixedContentPos: false,
			iframe: {
				markup: '<div class="mfp-iframe-scaler">'+
				'<div class="mfp-close"></div>'+
				'<iframe class="mfp-iframe" frameborder="0" allowfullscreen></iframe>'+
				'</div>',
				patterns: {
					youtube: {
						index: 'youtube.com/',
						id: 'v=',
						src: '//www.youtube.com/embed/%id%?autoplay=1&rel=0'
					}
				},
				srcAction: 'iframe_src',
			}
		});

		// disable links
		$('div.freinddd_listttt a').on('click', function(e) {
			e.preventDefault();
		});

		$('#dropdown-notifications, #dropdown-settings').on('show.bs.dropdown', function(e) {
			var str = '<div class="dropdown-backdrop fade"></div>';
			$(str).appendTo('body');
			$('div.dropdown-backdrop').addClass('show');
		});

		$('#dropdown-notifications, #dropdown-settings').on('hide.bs.dropdown', function(e) {
			$('div.dropdown-backdrop').removeClass('show').remove();
		});

		$('.onclickk_opeennn_all').click( function(e) {
			$(this).hide();
			$('.leftt_all_iconss').show().css( 'top', '0' );
			$('.left_menusssss').show().css( 'bottom', '30px' );
			
			e.preventDefault();
		});

		$('.leftt_all_iconss').click( function(e) {
			$(this).hide().css( 'top', '100%' );
			$('.left_menusssss').hide().css( 'bottom', '-100%' );
			$('.onclickk_opeennn_all').show();
			e.preventDefault();
		});

		$('.friend_privacy_connect a.got_btn').on('click', function(e) {
			$('.friend_privacy_connect').hide();
			e.preventDefault();
		});

		var alert_timeout;

		var clipboard_copyyy_linkk = new ClipboardJS('div.copyyy_linkk', {
			text: function(trigger) {
				return $(trigger).find('span').text();
			}
		});

		clipboard_copyyy_linkk.on('success', function(e) {
			var $alert = '<div class="settings-alert"><i class="fa fa-check-circle"></i><strong>Copied</strong></div>';
			$($alert).prependTo('#profile-content');
			alert_timeout = setTimeout( function() {
				$('div.settings-alert').remove();
			}, 2000);

			e.clearSelection();
		});

		var clipboard_messaagee_ooneee = new ClipboardJS('div.messaagee_ooneee', {
			text: function(trigger) {
				return $(trigger).find('div.meassge_textt').text();
			}
		});

		clipboard_messaagee_ooneee.on('success', function(e) {
			var $alert = '<div class="settings-alert"><i class="fa fa-check-circle"></i><strong>Copied</strong></div>';
			$($alert).prependTo('#profile-content');
			alert_timeout = setTimeout( function() {
				$('div.settings-alert').remove();
			}, 2000);

			e.clearSelection();
		});

		var clipboard_copy_btn = new ClipboardJS('div.pc--cta-actions span.copy', {
			text: function(trigger) {
				return $(trigger).attr('data-clipboard-text');
			}
		});

		clipboard_copy_btn.on('success', function(e) {
			$(e.trigger).html('Copied');
			e.clearSelection();
		});

		function bottom_menusss() {
			var top_space = $('#profile-header').outerHeight();

			if( $('div.freinddd_listttt').length ) {
				top_space = 160.8;
			}

			$('div.left_side_barr div.bottom_menusss').sticky({
				topSpacing: top_space + 10,
				bottomSpacing: 200
			});
		}

		if( $('div.left_side_barr div.bottom_menusss').length ) {
			bottom_menusss();
		}
		
		function header_sticky() {
			$('#profile-header').sticky({
				topSpacing: 0,
				zIndex: 1050
			});

			$('.test-header.sticky').sticky({
				topSpacing: 0,
				zIndex: 1050
			});

			$('#site-header').sticky({
				topSpacing: 0,
				zIndex: 1050
			});

			function scrollDetection() {
				var scrollPosition = 0;
	
				$(window).scroll(function () {
					var header_height = $('#profile-header').outerHeight();

					if( !header_height && $('.test-header.sticky').length ) {
						header_height = $('.test-header.sticky').outerHeight();
					} else if( !header_height && $('#site-header').length ) {
						header_height = $('#site-header').outerHeight();
					}

					var cursorPosition = $(this).scrollTop();
					
					if( cursorPosition > header_height ) {
						if( cursorPosition > scrollPosition ) {
							$('body').removeClass('scroll-up').addClass('scroll-down');
						} else if ( cursorPosition < scrollPosition ) {
							$('body').removeClass('scroll-down').addClass('scroll-up');
						}
					} else {
						$('body').removeClass('scroll-up scroll-down');
					}
					
					scrollPosition = cursorPosition;
				});
			}

			scrollDetection();
		}

		header_sticky();

		function sidenav_init() {
			var $content = $( '#profile-content' ),
				$button = $( 'button.profile-header__toggler' ),
				last_position = null,
				isOpen = false;

			initEvents();

			function initEvents() {
				$button.on('click', function(e) {
					e.preventDefault();
					toggleMenu();
				});

				$content.on('click', function(e) {
					if( isOpen && e.target !== $button ) {
						toggleMenu();
					}
				});
			}

			function toggleMenu() {
				if( isOpen ) {
					$('body').removeClass('open-sidenav');
					$('#main.profile-main, body').css('height', 'auto');
					$('#profile-sidenav').css('min-height', '100%');

					if( last_position ) {
						$(window).scrollTop(last_position);
					}
				} else {
					var h = window.innerHeight || $(window).height();
					last_position = $(window).scrollTop();
					
					$('#profile-content').css('top', '-' + last_position + 'px');
					$('body').addClass('open-sidenav');
					$('#main.profile-main, body').css('height', h-40);
					$('#profile-sidenav').css('min-height', h);
				}

				isOpen = !isOpen;
			}
		}

		sidenav_init();

		function server_error() {
			$('div#server-error').click( function(e) {
				if( !$(e.target).hasClass('server-error-inner') && $(e.target).parents('.server-error-inner').length == 0 ) {
					$('div#server-error').hide();
				}
			});
		}

		if( $('div#server-error').length ) {
			server_error();
		}

		function basic_settings() {
			$(document).on( 'keypress', 'div.dropdown-email-opt.cloneable input', function(e) {
				var wrap = $(this).parent();
				var clone_html = '<div class="dropdown dropdown-email-opt cloneable">' + wrap.html() + '</div>';

				wrap.removeClass('cloneable');
				$(clone_html).appendTo( $('div.form-group-email .col-sm-8') );
				$('div.dropdown-email-opt.primary button.dropdown-toggle').removeClass('d-none');
			});

			$('div.form-group-email').on('click', 'a.delete-email', function(e) {
				$(this).parents('div.dropdown-email-opt').remove();
				
				if( $('div.form-group-email div.dropdown-email-opt').length == 2 ) {
					$('div.dropdown-email-opt.primary button.dropdown-toggle').addClass('d-none');
				}

				e.preventDefault();
			});

			$('#setpassword form').validate({
				rules: {
					'password': 'required'
				},
				onfocusout: false,
				errorPlacement: function(error, element) {
					return true;
				},
				onkeyup: function(element) {
					$(element).valid();

					if( $('#setpassword form').valid() ) {
						$('#setpassword form').find( 'button[type="submit"]' ).removeAttr('disabled');
					} else {
						$('#setpassword form').find( 'button[type="submit"]' ).attr('disabled', 'disabled');
					}
				}
			});
		}

		if( $('div.settings-info').length ) {
			basic_settings();
		}

		function all_friendss() {
			var $container = $('#all_friendss-grid').infiniteScroll({
				path: function() {
					return 'friends.json';
				},
				responseType: 'text',
				history: false,
			});

			$container.on( 'request.infiniteScroll', function( event, path ) {
				$('div.all_friendss div.lds-spinner').show();
			});

			$container.on( 'load.infiniteScroll', function( event, response ) {
				var data = JSON.parse( response );
				
				if( data ) {
					var itemsHTML = '';
					$.each( data, function(i, item) {
						itemsHTML += '<div class="col-lg-4 col-md-6 col-sm-6 col-12 paddd_twntyy">';
							itemsHTML += '<div class="friend_pro">';
								itemsHTML += '<a href="' +item.url+ '">';
									itemsHTML += '<img src="' +item.photo+ '" class="rounded">';
									itemsHTML += '<strong class="friend_name">' +item.name+ '</strong>';
								itemsHTML += '</a>';
							itemsHTML += '</div><!-- .friend_pro -->';
						itemsHTML += '</div>';
					});
				}

				var $items = $( itemsHTML );
				$('div.all_friendss div.lds-spinner').hide();
				$container.infiniteScroll( 'appendItems', $items );
			});
		}

		if( $('div.all_friendss').length ) {
			all_friendss();
		}

		function freinddd_listttt() {
			$('img.lazyload').lazyload();
		}

		if( $('div.freinddd_listttt').length ) {
			freinddd_listttt();
		}

		function help_init() {
			if( !$('div.help-container').length ) {
				return;
			}

			$('.help-container a.back').on('click', function(e) {
				var target = $(this).attr('href');

				if( target ) {
					$(this).parents('div.help-step').addClass('d-none');
					$(target).removeClass('d-none');
				}

				e.preventDefault();
			});

			$('.help-container .hfield input[name="general-contact"]').on('click', function(e) {
				var value = $(this).val();
				var window_width = $(window).width();

				if( value == 'feedback' ) {
					$('#help-begin').addClass('d-none');
					$('#help-feedback').removeClass('d-none');

				} else if( value == 'report' ) {
					$('#help-begin').addClass('d-none');
					$('#help-report').removeClass('d-none');

				} else if( value == 'contact' ) {
					$('#help-begin').addClass('d-none');
					$('#help-contact').removeClass('d-none');
				
				} else if( value == 'support' ) {
					$('#help-begin').addClass('d-none');
					$('#help-support').removeClass('d-none');
				
				} else if( value == 'billing' ) {
					$('#help-begin').addClass('d-none');
					$('#help-billing').removeClass('d-none');
				
				} else if( value == 'refund' ) {
					$('#help-begin').addClass('d-none');
					$('#help-refund').removeClass('d-none');

				}

				if( window_width < 767 ) {
					var go_position = $('#help-container').offset().top - 90;
                    $('html,body').animate({ scrollTop: go_position }, 300);
				}
			});

			$('#help-refund .hfield input[name="help-refund"]').on('click', function(e) {
				var value = $(this).val();
				var window_width = $(window).width();

				if( value == 1 || value == 2 ) {
					$('#help-refund').addClass('d-none');
					$('#refund-msg').removeClass('d-none');
				} else if( value == 4 ) {
					$('#help-refund').addClass('d-none');
					$('#refund-noteligible').removeClass('d-none');
				}

				if( window_width < 767 ) {
					var go_position = $('#help-container').offset().top - 90;
                    $('html,body').animate({ scrollTop: go_position }, 300);
				}
			});

			$('#refund-msg .hsubmit button').on('click', function(e) {
				$('#refund-msg').addClass('d-none');
				$('#help-refund-msg-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#refund-noteligible .hsubmit button').on('click', function(e) {
				$('#refund-noteligible').addClass('d-none');
				$('#help-refund-noteligible-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#help-feedback .hsubmit button').on('click', function(e) {
				$('#help-feedback').addClass('d-none');
				$('#help-feedback-thanks').removeClass('d-none');

				e.preventDefault();
			});

			$('#help-report .hsubmit button').on('click', function(e) {
				$('#help-report').addClass('d-none');
				$('#help-report-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#help-contact .hsubmit button').on('click', function(e) {
				$('#help-contact').addClass('d-none');
				$('#help-contact-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#help-support .hsubmit button').on('click', function(e) {
				$('#help-support').addClass('d-none');
				$('#help-support-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#help-billing .hsubmit button').on('click', function(e) {
				$('#help-billing').addClass('d-none');
				$('#help-billing-thanks').removeClass('d-none');
				
				e.preventDefault();
			});
		}

		help_init();

		function test_init() {
			$('form.test-form .ques_oneeee input[type="radio"], form.test-form .ques_oneeee select').on('change', function(e) {
				$('html, body').animate({
					scrollTop: $(window).scrollTop() + 200
				});
			});
		}

		test_init();

		function scroll_nav() {
			$('div.scroll-nav').sticky({
				topSpacing: 170,
				bottomSpacing: 1010,
				wrapperClassName: 'scroll-nav-sticky-wrapper'
			});

			$('.tom-content div.left_menusssss a, div.scroll-nav a').on('click', function(e) {
				var target = $(this).attr('href');
				var checkURL = target.match(/#([^\/]+)$/i);
				
				if( checkURL[0] ) {
                    var go_position = $(target).offset().top - 100;
                    $('html,body').animate({
						scrollTop: go_position
					}, 400);
				}
				
				e.preventDefault();
			});

			$(window).on('activate.bs.scrollspy', function(e, obj) {
				var current_heading = $('div.scroll-nav a[href="' +obj.relatedTarget+ '"]').html();
				
				$('div.onclickk_opeennn_all').html( '<a href="#">' + current_heading + '</a>' );
				$('div.left_menusssss a').removeClass('active');
				$('div.left_menusssss a[href="' +obj.relatedTarget+ '"]').addClass('active');
			});

			$('body').scrollspy({
				target: 'div.scroll-nav' ,
				offset: 180
			});
		}

		if( $('div.scroll-nav').length ) {
			scroll_nav();
		}
	});
})(jQuery);$(".navbar-toggler").click(function(){
	$(".main").toggleClass("nav_fix");
	$(".mobile_nav").toggleClass("mob_fix");
});

$(".friend_section").click(function(){
	$(".main").removeClass("nav_fix");
	$(".mobile_nav").removeClass("mob_fix");
});

(function($) {
	$(document).ready( function() {
		$('[data-toggle="popover"]').popover();

		$('button.toggle-menu').on('click', function(e) {
			$('#mobile-menu').toggleClass('show');
		});

		$('#mobile-menu').on('click', function(e) {
			if( e.target !== $('#mobile-menu div.inner') && $(e.target).parents('div.inner').length == 0 ) {
				$('#mobile-menu').removeClass('show');
			}
		});

		$('.popup-video a').magnificPopup({
			type: 'iframe',
			mainClass: 'mfp-fade',
			removalDelay: 160,
			preloader: false,
			disableOn: 300,
			fixedContentPos: false,
			iframe: {
				markup: '<div class="mfp-iframe-scaler">'+
				'<div class="mfp-close"></div>'+
				'<iframe class="mfp-iframe" frameborder="0" allowfullscreen></iframe>'+
				'</div>',
				patterns: {
					youtube: {
						index: 'youtube.com/',
						id: 'v=',
						src: '//www.youtube.com/embed/%id%?autoplay=1&rel=0'
					}
				},
				srcAction: 'iframe_src',
			}
		});

		// disable links
		$('div.freinddd_listttt a').on('click', function(e) {
			e.preventDefault();
		});

		$('#dropdown-notifications, #dropdown-settings').on('show.bs.dropdown', function(e) {
			var str = '<div class="dropdown-backdrop fade"></div>';
			$(str).appendTo('body');
			$('div.dropdown-backdrop').addClass('show');
		});

		$('#dropdown-notifications, #dropdown-settings').on('hide.bs.dropdown', function(e) {
			$('div.dropdown-backdrop').removeClass('show').remove();
		});

		$('.onclickk_opeennn_all').click( function(e) {
			$(this).hide();
			$('.leftt_all_iconss').show().css( 'top', '0' );
			$('.left_menusssss').show().css( 'bottom', '30px' );
			
			e.preventDefault();
		});

		$('.leftt_all_iconss').click( function(e) {
			$(this).hide().css( 'top', '100%' );
			$('.left_menusssss').hide().css( 'bottom', '-100%' );
			$('.onclickk_opeennn_all').show();
			e.preventDefault();
		});

		$('.friend_privacy_connect a.got_btn').on('click', function(e) {
			$('.friend_privacy_connect').hide();
			e.preventDefault();
		});

		var alert_timeout;

		var clipboard_copyyy_linkk = new ClipboardJS('div.copyyy_linkk', {
			text: function(trigger) {
				return $(trigger).find('span').text();
			}
		});

		clipboard_copyyy_linkk.on('success', function(e) {
			var $alert = '<div class="settings-alert"><i class="fa fa-check-circle"></i><strong>Copied</strong></div>';
			$($alert).prependTo('#profile-content');
			alert_timeout = setTimeout( function() {
				$('div.settings-alert').remove();
			}, 2000);

			e.clearSelection();
		});

		var clipboard_messaagee_ooneee = new ClipboardJS('div.messaagee_ooneee', {
			text: function(trigger) {
				return $(trigger).find('div.meassge_textt').text();
			}
		});

		clipboard_messaagee_ooneee.on('success', function(e) {
			var $alert = '<div class="settings-alert"><i class="fa fa-check-circle"></i><strong>Copied</strong></div>';
			$($alert).prependTo('#profile-content');
			alert_timeout = setTimeout( function() {
				$('div.settings-alert').remove();
			}, 2000);

			e.clearSelection();
		});

		var clipboard_copy_btn = new ClipboardJS('div.pc--cta-actions span.copy', {
			text: function(trigger) {
				return $(trigger).attr('data-clipboard-text');
			}
		});

		clipboard_copy_btn.on('success', function(e) {
			$(e.trigger).html('Copied');
			e.clearSelection();
		});

		function bottom_menusss() {
			var top_space = $('#profile-header').outerHeight();

			if( $('div.freinddd_listttt').length ) {
				top_space = 160.8;
			}

			$('div.left_side_barr div.bottom_menusss').sticky({
				topSpacing: top_space + 10,
				bottomSpacing: 200
			});
		}

		if( $('div.left_side_barr div.bottom_menusss').length ) {
			bottom_menusss();
		}
		
		function header_sticky() {
			$('#profile-header').sticky({
				topSpacing: 0,
				zIndex: 1050
			});

			$('.test-header.sticky').sticky({
				topSpacing: 0,
				zIndex: 1050
			});

			$('#site-header').sticky({
				topSpacing: 0,
				zIndex: 1050
			});

			function scrollDetection() {
				var scrollPosition = 0;
	
				$(window).scroll(function () {
					var header_height = $('#profile-header').outerHeight();

					if( !header_height && $('.test-header.sticky').length ) {
						header_height = $('.test-header.sticky').outerHeight();
					} else if( !header_height && $('#site-header').length ) {
						header_height = $('#site-header').outerHeight();
					}

					var cursorPosition = $(this).scrollTop();
					
					if( cursorPosition > header_height ) {
						if( cursorPosition > scrollPosition ) {
							$('body').removeClass('scroll-up').addClass('scroll-down');
						} else if ( cursorPosition < scrollPosition ) {
							$('body').removeClass('scroll-down').addClass('scroll-up');
						}
					} else {
						$('body').removeClass('scroll-up scroll-down');
					}
					
					scrollPosition = cursorPosition;
				});
			}

			scrollDetection();
		}

		header_sticky();

		function sidenav_init() {
			var $content = $( '#profile-content' ),
				$button = $( 'button.profile-header__toggler' ),
				last_position = null,
				isOpen = false;

			initEvents();

			function initEvents() {
				$button.on('click', function(e) {
					e.preventDefault();
					toggleMenu();
				});

				$content.on('click', function(e) {
					if( isOpen && e.target !== $button ) {
						toggleMenu();
					}
				});
			}

			function toggleMenu() {
				if( isOpen ) {
					$('body').removeClass('open-sidenav');
					$('#main.profile-main, body').css('height', 'auto');
					$('#profile-sidenav').css('min-height', '100%');

					if( last_position ) {
						$(window).scrollTop(last_position);
					}
				} else {
					var h = window.innerHeight || $(window).height();
					last_position = $(window).scrollTop();
					
					$('#profile-content').css('top', '-' + last_position + 'px');
					$('body').addClass('open-sidenav');
					$('#main.profile-main, body').css('height', h-40);
					$('#profile-sidenav').css('min-height', h);
				}

				isOpen = !isOpen;
			}
		}

		sidenav_init();

		function server_error() {
			$('div#server-error').click( function(e) {
				if( !$(e.target).hasClass('server-error-inner') && $(e.target).parents('.server-error-inner').length == 0 ) {
					$('div#server-error').hide();
				}
			});
		}

		if( $('div#server-error').length ) {
			server_error();
		}

		function basic_settings() {
			$(document).on( 'keypress', 'div.dropdown-email-opt.cloneable input', function(e) {
				var wrap = $(this).parent();
				var clone_html = '<div class="dropdown dropdown-email-opt cloneable">' + wrap.html() + '</div>';

				wrap.removeClass('cloneable');
				$(clone_html).appendTo( $('div.form-group-email .col-sm-8') );
				$('div.dropdown-email-opt.primary button.dropdown-toggle').removeClass('d-none');
			});

			$('div.form-group-email').on('click', 'a.delete-email', function(e) {
				$(this).parents('div.dropdown-email-opt').remove();
				
				if( $('div.form-group-email div.dropdown-email-opt').length == 2 ) {
					$('div.dropdown-email-opt.primary button.dropdown-toggle').addClass('d-none');
				}

				e.preventDefault();
			});

			$('#setpassword form').validate({
				rules: {
					'password': 'required'
				},
				onfocusout: false,
				errorPlacement: function(error, element) {
					return true;
				},
				onkeyup: function(element) {
					$(element).valid();

					if( $('#setpassword form').valid() ) {
						$('#setpassword form').find( 'button[type="submit"]' ).removeAttr('disabled');
					} else {
						$('#setpassword form').find( 'button[type="submit"]' ).attr('disabled', 'disabled');
					}
				}
			});
		}

		if( $('div.settings-info').length ) {
			basic_settings();
		}

		function all_friendss() {
			var $container = $('#all_friendss-grid').infiniteScroll({
				path: function() {
					return 'friends.json';
				},
				responseType: 'text',
				history: false,
			});

			$container.on( 'request.infiniteScroll', function( event, path ) {
				$('div.all_friendss div.lds-spinner').show();
			});

			$container.on( 'load.infiniteScroll', function( event, response ) {
				var data = JSON.parse( response );
				
				if( data ) {
					var itemsHTML = '';
					$.each( data, function(i, item) {
						itemsHTML += '<div class="col-lg-4 col-md-6 col-sm-6 col-12 paddd_twntyy">';
							itemsHTML += '<div class="friend_pro">';
								itemsHTML += '<a href="' +item.url+ '">';
									itemsHTML += '<img src="' +item.photo+ '" class="rounded">';
									itemsHTML += '<strong class="friend_name">' +item.name+ '</strong>';
								itemsHTML += '</a>';
							itemsHTML += '</div><!-- .friend_pro -->';
						itemsHTML += '</div>';
					});
				}

				var $items = $( itemsHTML );
				$('div.all_friendss div.lds-spinner').hide();
				$container.infiniteScroll( 'appendItems', $items );
			});
		}

		if( $('div.all_friendss').length ) {
			all_friendss();
		}

		function freinddd_listttt() {
			$('img.lazyload').lazyload();
		}

		if( $('div.freinddd_listttt').length ) {
			freinddd_listttt();
		}

		function help_init() {
			if( !$('div.help-container').length ) {
				return;
			}

			$('.help-container a.back').on('click', function(e) {
				var target = $(this).attr('href');

				if( target ) {
					$(this).parents('div.help-step').addClass('d-none');
					$(target).removeClass('d-none');
				}

				e.preventDefault();
			});

			$('.help-container .hfield input[name="general-contact"]').on('click', function(e) {
				var value = $(this).val();
				var window_width = $(window).width();

				if( value == 'feedback' ) {
					$('#help-begin').addClass('d-none');
					$('#help-feedback').removeClass('d-none');

				} else if( value == 'report' ) {
					$('#help-begin').addClass('d-none');
					$('#help-report').removeClass('d-none');

				} else if( value == 'contact' ) {
					$('#help-begin').addClass('d-none');
					$('#help-contact').removeClass('d-none');
				
				} else if( value == 'support' ) {
					$('#help-begin').addClass('d-none');
					$('#help-support').removeClass('d-none');
				
				} else if( value == 'billing' ) {
					$('#help-begin').addClass('d-none');
					$('#help-billing').removeClass('d-none');
				
				} else if( value == 'refund' ) {
					$('#help-begin').addClass('d-none');
					$('#help-refund').removeClass('d-none');

				}

				if( window_width < 767 ) {
					var go_position = $('#help-container').offset().top - 90;
                    $('html,body').animate({ scrollTop: go_position }, 300);
				}
			});

			$('#help-refund .hfield input[name="help-refund"]').on('click', function(e) {
				var value = $(this).val();
				var window_width = $(window).width();

				if( value == 1 || value == 2 ) {
					$('#help-refund').addClass('d-none');
					$('#refund-msg').removeClass('d-none');
				} else if( value == 4 ) {
					$('#help-refund').addClass('d-none');
					$('#refund-noteligible').removeClass('d-none');
				}

				if( window_width < 767 ) {
					var go_position = $('#help-container').offset().top - 90;
                    $('html,body').animate({ scrollTop: go_position }, 300);
				}
			});

			$('#refund-msg .hsubmit button').on('click', function(e) {
				$('#refund-msg').addClass('d-none');
				$('#help-refund-msg-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#refund-noteligible .hsubmit button').on('click', function(e) {
				$('#refund-noteligible').addClass('d-none');
				$('#help-refund-noteligible-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#help-feedback .hsubmit button').on('click', function(e) {
				$('#help-feedback').addClass('d-none');
				$('#help-feedback-thanks').removeClass('d-none');

				e.preventDefault();
			});

			$('#help-report .hsubmit button').on('click', function(e) {
				$('#help-report').addClass('d-none');
				$('#help-report-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#help-contact .hsubmit button').on('click', function(e) {
				$('#help-contact').addClass('d-none');
				$('#help-contact-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#help-support .hsubmit button').on('click', function(e) {
				$('#help-support').addClass('d-none');
				$('#help-support-thanks').removeClass('d-none');
				
				e.preventDefault();
			});

			$('#help-billing .hsubmit button').on('click', function(e) {
				$('#help-billing').addClass('d-none');
				$('#help-billing-thanks').removeClass('d-none');
				
				e.preventDefault();
			});
		}

		help_init();

		function test_init() {
			$('form.test-form .ques_oneeee input[type="radio"], form.test-form .ques_oneeee select').on('change', function(e) {
				$('html, body').animate({
					scrollTop: $(window).scrollTop() + 200
				});
			});
		}

		test_init();

		function scroll_nav() {
			$('div.scroll-nav').sticky({
				topSpacing: 170,
				bottomSpacing: 1010,
				wrapperClassName: 'scroll-nav-sticky-wrapper'
			});

			$('.tom-content div.left_menusssss a, div.scroll-nav a').on('click', function(e) {
				var target = $(this).attr('href');
				var checkURL = target.match(/#([^\/]+)$/i);
				
				if( checkURL[0] ) {
                    var go_position = $(target).offset().top - 100;
                    $('html,body').animate({
						scrollTop: go_position
					}, 400);
				}
				
				e.preventDefault();
			});

			$(window).on('activate.bs.scrollspy', function(e, obj) {
				var current_heading = $('div.scroll-nav a[href="' +obj.relatedTarget+ '"]').html();
				
				$('div.onclickk_opeennn_all').html( '<a href="#">' + current_heading + '</a>' );
				$('div.left_menusssss a').removeClass('active');
				$('div.left_menusssss a[href="' +obj.relatedTarget+ '"]').addClass('active');
			});

			$('body').scrollspy({
				target: 'div.scroll-nav' ,
				offset: 180
			});
		}

		if( $('div.scroll-nav').length ) {
			scroll_nav();
		}
	});
})(jQuery);